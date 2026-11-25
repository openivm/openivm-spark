package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

private[common] trait CdfWatermarkBackend {
  def ensureTables(spark: SparkSession): Unit
  def get(spark: SparkSession, viewName: String, source: String): Option[Long]
  def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long]
  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit
  def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit
  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit
  def removeForView(spark: SparkSession, viewName: String): Unit
}

private[common] object CdfWatermarkBackend {
  def forSession(spark: SparkSession): CdfWatermarkBackend =
    if (FeatureGate.deltaCatalogEnabled(spark)) GroupCommitDeltaCdfWatermarkBackend
    else RocksDbCdfWatermarkBackend
}

/** Delta CDF watermarks whose concurrent `putAll` calls in this JVM share one MERGE per group commit. */
private[common] object GroupCommitDeltaCdfWatermarkBackend extends CdfWatermarkBackend {
  private val Delegate = DeltaCdfWatermarkBackend
  private val ViewName = "view_name"
  private val Source   = "source"
  private val Version  = "version"

  private val schema = StructType(
    Seq(
      StructField(ViewName, StringType, nullable = false),
      StructField(Source, StringType, nullable = false),
      StructField(Version, LongType, nullable = false)
    )
  )

  private final case class Put(viewName: String, versionsBySource: Map[String, Long])

  private def path(spark: SparkSession): String = FeatureGate.catalogPath(spark).stripSuffix("/") + "/cdf_watermarks"

  override def ensureTables(spark: SparkSession): Unit = Delegate.ensureTables(spark)

  override def get(spark: SparkSession, viewName: String, source: String): Option[Long] =
    Delegate.get(spark, viewName, source)

  override def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long] =
    Delegate.getAll(spark, viewName, sources)

  override def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    putAll(spark, viewName, Map(source -> version))

  override def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit =
    if (versionsBySource.nonEmpty)
      DeltaGroupCommit
        .forPath[Put](path(spark))
        .submit(Put(viewName, versionsBySource))(p => Delegate.putAll(spark, p.viewName, p.versionsBySource))(
          putBatch(spark, _)
        )

  /** One MERGE with the same per-row semantics as sequential `putAll`s: a watermark only moves forward. */
  private def putBatch(spark: SparkSession, puts: Seq[Put]): Unit =
    OpenIvmMetrics.time("catalog.cdf_watermark.put_all_batch") {
      val rows = puts
        .flatMap(p => p.versionsBySource.toSeq.map { case (source, version) => (p.viewName, source) -> version })
        .groupBy(_._1)
        .toSeq
        .map { case ((view, source), versions) => Row(view, source, versions.map(_._2).max) }
      val views = puts.map(_.viewName).distinct
      Delegate.withDeltaRetry {
        Delegate.ensureTables(spark)
        val incoming = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
        DeltaTable
          .forPath(spark, path(spark))
          .as("target")
          .merge(
            incoming.as("incoming"),
            col(s"target.$ViewName").isin(views: _*) &&
              col(s"target.$ViewName") === col(s"incoming.$ViewName") &&
              col(s"target.$Source") === col(s"incoming.$Source")
          )
          .whenMatched(s"incoming.$Version > target.$Version")
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
      }
    }

  override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit =
    Delegate.removeForBaseTable(spark, baseTable)

  override def removeForView(spark: SparkSession, viewName: String): Unit = Delegate.removeForView(spark, viewName)
}
