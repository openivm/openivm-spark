package org.openivm.spark.streaming

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.AnalysisException
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class StreamingTablePathResolutionSpec extends AnyFunSpec with StreamingTableTestFixture {
  override protected def sessionExtensions: String =
    s"${super.sessionExtensions},${classOf[StreamingNamespaceAliasExtension].getName}"
  override protected def sessionCatalog: String = classOf[StreamingAliasDeltaCatalog].getName

  describe("catalog-native streaming target locations") {
    it("resolves a catalog namespace alias before planning the target location") {
      spark.sql("CREATE DATABASE strt_encoded_namespace").collect()
      val target  = Seq("strt_friendly_alias", "123_target")
      val encoded = TableIdentifier("123_target", Some("strt_encoded_namespace"), Some("spark_catalog"))
      intercept[AnalysisException] {
        spark.sessionState.catalog.defaultTablePath(
          TableIdentifier("123_target", Some("strt_friendly_alias"), Some("spark_catalog"))
        )
      }

      val expected =
        StreamingTableMetadata.normalizePath(spark, new Path(spark.sessionState.catalog.defaultTablePath(encoded)))
      StreamingTableMetadata.plannedTargetPath(spark, target, None) shouldBe Some(expected)
      val explicit = new File("target", "strt-alias-explicit-location").getAbsolutePath
      StreamingTableMetadata.validateRequestedLocation(spark, explicit, target)
      StreamingTableMetadata.plannedTargetPath(spark, target, Some(explicit)) shouldBe
        Some(StreamingTableMetadata.normalizePath(spark, explicit))
      val root = spark.catalog.getDatabase("strt_encoded_namespace").locationUri
      intercept[AnalysisException] {
        StreamingTableMetadata.validateRequestedLocation(spark, root, target)
      }
      StreamingTableMetadata.canonicalIdentity(spark, target) should not be
        StreamingTableMetadata.canonicalIdentity(spark, Seq("strt_encoded_namespace", "123_target"))
      spark.sql("DROP DATABASE strt_encoded_namespace CASCADE").collect()
    }

    it("preserves catalog errors rather than guessing a configured warehouse path") {
      val key      = "spark.openivm.managedTablesRoot"
      val previous = spark.conf.getOption(key)
      try {
        spark.conf.set(key, new File("target", "strt-guess-root").getAbsolutePath)
        intercept[AnalysisException] {
          StreamingTableMetadata.plannedTargetPath(spark, Seq("strt_missing_namespace", "target"), None)
        }
      } finally
        previous match {
          case Some(value) => spark.conf.set(key, value)
          case None        => spark.conf.unset(key)
        }
      StreamingTableMetadata.plannedTargetPath(spark, Seq("other_catalog", "namespace", "target"), None) shouldBe None
    }

    it("matches the planned location to a created target with a quoted numeric-leading name") {
      val source = "strt_path_source"
      val target = "123_path_target"
      createSource(source)
      val planned = StreamingTableMetadata.plannedTargetPath(spark, Seq(target), None).get
      val status  = createStreaming(target, readStream(source), s"SELECT id, value, part FROM STREAM $source")
      status.queryId should not be empty
      targetPath(target) shouldBe planned
      StreamingTableManager.drop(spark, Seq(target), ifExists = false)
    }

    it("refuses to bind a target created at a different location") {
      val source = "strt_path_mismatch_source"
      val target = "strt_path_mismatch_target"
      createSource(source)
      val query = readStream(source)
      val spec =
        StreamingTableSpec(Seq(target), s"SELECT id, value, part FROM STREAM $source", query.queryExecution.logical)
      val wrongPath =
        StreamingTableMetadata.normalizePath(spark, new File("target", "strt-wrong-location").getAbsolutePath)
      val error = intercept[AnalysisException] {
        StreamingTableMetadata.createOwnedTarget(spark, spec, query.schema, wrongPath)
      }
      error.getMessage should include("not its planned location")
      spark.catalog.tableExists(target) shouldBe true
      val created = StreamingTableMetadata.interruptedCreation(spark, Seq(target)).get
      pathExists(created.dataPath) shouldBe true
      pathExists(created.checkpointLocation) shouldBe false
      spark.sql(s"DROP TABLE `$target`").collect()
    }
  }
}

class StreamingAliasDeltaCatalog extends DeltaCatalog {
  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] =
    super.loadNamespaceMetadata(
      if (namespace.toSeq == Seq("strt_friendly_alias")) Array("strt_encoded_namespace") else namespace
    )
}

class StreamingNamespaceAliasExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectResolutionRule { _ =>
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
          case namespace: ResolvedNamespace
              if namespace.catalog.name().equalsIgnoreCase("spark_catalog") &&
                namespace.namespace == Seq("strt_friendly_alias") =>
            namespace.copy(namespace = Seq("strt_encoded_namespace"))
        }
      }
    }
}
