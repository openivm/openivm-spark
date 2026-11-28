package org.openivm.spark.parity

import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.openivm.spark.common.{
  DeltaTableVersion,
  MvCatalog,
  MvMetadata,
  PublicOutputNames,
  RefreshTypeCode,
  SourceVersionChangeBatch,
  SourceVersionDelta,
  StagingCatalog,
  StagingDeltaView
}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

class PublicMvSchemaSpec    extends PublicMvSchemaScenarios with InterceptMode
class PublicMvSchemaCdfSpec extends PublicMvSchemaScenarios with CdfMode

abstract class PublicMvSchemaScenarios extends IvmParitySpecBase("public-mv-schema") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map(
      "spark.openivm.catalogPreservesColumnCase" -> "true",
      "spark.openivm.managedTablesRoot"          -> s"$warehouseDir/Tables"
    )

  private def metadata(name: String): MvMetadata =
    MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(s"default.$name")).get

  override protected def refreshMv(name: String): Unit =
    super.refreshMv(s"default.$name")

  private def assertPublicResult(name: String, query: String): Unit = {
    val expected = sql(query)
    val actual   = spark.table(name)
    actual.schema.fields.map(field => field.name -> field.dataType).toSeq shouldBe
      expected.schema.fields.map(field => field.name -> field.dataType).toSeq
    withClue(s"$name unexpected public rows: ") {
      actual.exceptAll(expected).limit(8).collect().toSeq shouldBe empty
    }
    withClue(s"$name missing public rows: ") {
      expected.exceptAll(actual).limit(8).collect().toSeq shouldBe empty
    }
  }

  private def assertBackingState(name: String, query: String): Unit = {
    assertPublicResult(name, query)
    val meta = metadata(name)
    meta.refreshType should not be RefreshTypeCode.FullRefresh
    meta.usesBackingDataTable shouldBe true
    meta.location should endWith(s"/default/${name}__ivm_data")
    spark.sessionState.catalog
      .getTableMetadata(spark.sessionState.sqlParser.parseTableIdentifier(name))
      .tableType shouldBe CatalogTableType.VIEW
    val userNames = sql(query).schema.fieldNames.toSeq
    val physical  = spark.read.format("delta").load(meta.location).schema.fieldNames.toSeq
    physical.filterNot(column => userNames.exists(_.equalsIgnoreCase(column))) should not be empty
  }

  private def assertMappedState(name: String, query: String, publics: Seq[String]): Unit = {
    assertBackingState(name, query)
    val view  = spark.sessionState.catalog.getTableMetadata(spark.sessionState.sqlParser.parseTableIdentifier(name))
    val names = PublicOutputNames.of(view.properties)
    names.pairs.map(_._1) shouldBe publics
    val physical = spark.read.format("delta").load(metadata(name).location).schema.fieldNames.toSeq
    names.pairs.foreach { case (public, column) =>
      column should startWith("ivm_alias_")
      column.toLowerCase(java.util.Locale.ROOT) should not include "openivm"
      physical should contain(column)
      physical.exists(_.equalsIgnoreCase(public)) shouldBe false
    }
  }

  describe("public MV schemas with compiler maintenance state") {
    it("retains null SUM state, public types and user-authored openivm column names across refresh") {
      sql("CREATE TABLE ps_amounts (id INT, bucket STRING, amount DECIMAL(10, 2)) USING DELTA")
      sql("INSERT INTO ps_amounts VALUES (1, 'a', NULL), (2, 'a', NULL), (3, 'b', 2.30)")
      val query =
        """SELECT bucket AS BucketKey, SUM(amount) AS openivm_user_total,
          |COUNT(amount) AS PresentCount, AVG(amount) AS AverageAmount
          |FROM ps_amounts GROUP BY bucket""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW default.ps_amounts_mv AS $query")
      assertMappedState("ps_amounts_mv", query, Seq("openivm_user_total"))

      sql("UPDATE ps_amounts SET amount = 4.25 WHERE id = 1")
      refreshMv("ps_amounts_mv")
      assertMappedState("ps_amounts_mv", query, Seq("openivm_user_total"))

      sql("DELETE FROM ps_amounts WHERE id IN (1, 3)")
      refreshMv("ps_amounts_mv")
      assertMappedState("ps_amounts_mv", query, Seq("openivm_user_total"))
    }

    it("keeps native helper-colliding aliases incremental in both spellings across min-affecting changes") {
      sql("CREATE TABLE ps_rowid (id INT, bucket STRING, amount INT) USING DELTA")
      sql("INSERT INTO ps_rowid VALUES (1, 'a', 5), (2, 'a', 3), (3, 'b', 7), (4, 'b', 9)")
      val queries = Seq(
        ("ps_rowid_lower_mv", "openivm_target_rowid", "SELECT bucket, MIN(amount) AS openivm_target_rowid"),
        ("ps_rowid_upper_mv", "OPENIVM_TARGET_ROWID", "SELECT bucket, MIN(amount) AS `OPENIVM_TARGET_ROWID`")
      ).map { case (name, public, select) =>
        (name, public, s"$select, COUNT(*) AS row_count FROM ps_rowid GROUP BY bucket")
      }
      queries.foreach { case (name, public, query) =>
        sql(s"CREATE MATERIALIZED VIEW default.$name AS $query")
        assertMappedState(name, query, Seq(public))
      }
      sql("DELETE FROM ps_rowid WHERE id = 2")
      sql("UPDATE ps_rowid SET amount = 1 WHERE id = 4")
      sql("INSERT INTO ps_rowid VALUES (5, 'c', 4)")
      queries.foreach { case (name, public, query) =>
        refreshMv(name)
        assertMappedState(name, query, Seq(public))
      }
    }

    it("preserves openivm_ literals and HAVING over a legitimate user alias across refresh") {
      sql("CREATE TABLE ps_literal (id INT, bucket STRING, tag STRING, amount INT) USING DELTA")
      sql(
        "INSERT INTO ps_literal VALUES (1, 'a', 'openivm_keep', 4), (2, 'a', 'other', 2), " +
          "(3, 'b', 'openivm_keep', 1), (4, 'b', 'openivm_keep', 5)"
      )
      val query =
        """SELECT bucket, SUM(CASE WHEN tag = 'openivm_keep' THEN amount ELSE 0 END) AS openivm_kept,
          |COUNT(*) AS row_count FROM ps_literal GROUP BY bucket
          |HAVING SUM(CASE WHEN tag = 'openivm_keep' THEN amount ELSE 0 END) > 3""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW default.ps_literal_mv AS $query")
      assertMappedState("ps_literal_mv", query, Seq("openivm_kept"))

      sql("INSERT INTO ps_literal VALUES (5, 'c', 'openivm_keep', 9)")
      sql("UPDATE ps_literal SET tag = 'other' WHERE id = 4")
      sql("DELETE FROM ps_literal WHERE id = 1")
      refreshMv("ps_literal_mv")
      assertMappedState("ps_literal_mv", query, Seq("openivm_kept"))
    }

    it("hides DISTINCT occurrence state while preserving duplicate and NULL transitions") {
      sql("CREATE TABLE ps_tags (id INT, bucket STRING, tag STRING) USING DELTA")
      sql("INSERT INTO ps_tags VALUES (1, 'a', 'x'), (2, 'a', 'x'), (3, 'a', NULL), (4, 'b', 'y')")
      val query = "SELECT DISTINCT bucket, tag FROM ps_tags"
      sql(s"CREATE MATERIALIZED VIEW default.ps_tags_mv AS $query")
      assertBackingState("ps_tags_mv", query)

      sql("DELETE FROM ps_tags WHERE id = 1")
      refreshMv("ps_tags_mv")
      assertBackingState("ps_tags_mv", query)

      sql("UPDATE ps_tags SET tag = CAST(NULL AS STRING) WHERE id = 2")
      sql("DELETE FROM ps_tags WHERE id = 4")
      refreshMv("ps_tags_mv")
      assertBackingState("ps_tags_mv", query)
    }

    it("keeps LEFT and FULL join keys private through multiplicity and NULL-match changes") {
      sql("CREATE TABLE ps_left (id INT, join_key INT, label STRING) USING DELTA")
      sql("CREATE TABLE ps_right (id INT, join_key INT, score INT) USING DELTA")
      sql("INSERT INTO ps_left VALUES (1, 10, 'a'), (2, 10, 'a'), (3, NULL, 'n'), (4, 20, 'b')")
      sql("INSERT INTO ps_right VALUES (1, 10, 5), (2, 30, 8), (3, NULL, 9)")
      val queries = Seq("LEFT", "FULL OUTER").zipWithIndex.map { case (join, index) =>
        s"ps_join_mv_$index" ->
          s"""SELECT COALESCE(l.join_key, r.join_key) AS visible_key, l.label, r.score
             |FROM ps_left l $join JOIN ps_right r ON l.join_key = r.join_key""".stripMargin
      }
      queries.foreach { case (name, query) =>
        sql(s"CREATE MATERIALIZED VIEW default.$name AS $query")
        assertBackingState(name, query)
      }
      sql("INSERT INTO ps_right VALUES (4, 20, 6), (5, 10, 5)")
      queries.foreach { case (name, query) =>
        refreshMv(name)
        assertBackingState(name, query)
      }
      sql("DELETE FROM ps_right WHERE join_key = 10")
      sql("UPDATE ps_left SET join_key = 30 WHERE id = 4")
      queries.foreach { case (name, query) =>
        refreshMv(name)
        assertBackingState(name, query)
      }
    }

    it("uses typed old-state snapshots when both sides of a downstream join change") {
      sql("CREATE TABLE ps_snapshot_amounts(id INT, bucket STRING, amount INT) USING DELTA")
      sql("CREATE TABLE ps_snapshot_labels(bucket STRING, label STRING) USING DELTA")
      sql("INSERT INTO ps_snapshot_amounts VALUES (1, 'a', 10), (2, 'b', 20)")
      sql("INSERT INTO ps_snapshot_labels VALUES ('a', 'first'), ('b', 'second')")
      val upstream = "SELECT bucket, SUM(amount) AS total FROM ps_snapshot_amounts GROUP BY bucket"
      val downstream =
        """SELECT a.bucket, a.total, l.label FROM ps_snapshot_upstream a
          |JOIN ps_snapshot_labels l ON a.bucket = l.bucket""".stripMargin
      sql(s"CREATE MATERIALIZED VIEW default.ps_snapshot_upstream AS $upstream")
      sql(s"CREATE MATERIALIZED VIEW default.ps_snapshot_downstream AS $downstream")
      assertBackingState("ps_snapshot_upstream", upstream)
      assertPublicResult("ps_snapshot_downstream", downstream)
      metadata("ps_snapshot_downstream").refreshType should not be RefreshTypeCode.FullRefresh

      sql("INSERT INTO ps_snapshot_amounts VALUES (3, 'a', 5), (4, 'c', 30)")
      sql("INSERT INTO ps_snapshot_labels VALUES ('a', 'other'), ('c', 'new')")
      refreshMv("ps_snapshot_upstream")
      val cascades =
        StagingCatalog.collectFor(spark, "default.ps_snapshot_downstream", Seq("default.ps_snapshot_upstream"))
      if (changeFeedMode == org.openivm.spark.common.ChangeFeedMode.Intercept)
        cascades should have size 1
      cascades.foreach { entry =>
        val frame      = spark.read.format("delta").load(entry.stagingPath)
        val signedRows = frame.select("bucket", "total", "openivm_multiplicity")
        val expectedRows = sql(
          """SELECT bucket, CAST(total AS BIGINT) AS total, sign AS openivm_multiplicity
              |FROM VALUES ('a', 10, -1), ('a', 15, 1), ('c', 30, 1)
              |AS changes(bucket, total, sign)""".stripMargin
        )
        signedRows.exceptAll(expectedRows).collect().toSeq shouldBe empty
        expectedRows.exceptAll(signedRows).collect().toSeq shouldBe empty
      }
      refreshMv("ps_snapshot_downstream")
      assertBackingState("ps_snapshot_upstream", upstream)
      assertPublicResult("ps_snapshot_downstream", downstream)

      if (changeFeedMode == org.openivm.spark.common.ChangeFeedMode.Intercept)
        sql(
          "ALTER TABLE default.ps_snapshot_upstream__ivm_data " +
            "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')"
        )
      sql("DELETE FROM ps_snapshot_amounts WHERE id = 1")
      sql("DELETE FROM ps_snapshot_labels WHERE label = 'first'")
      refreshMv("ps_snapshot_upstream")
      refreshMv("ps_snapshot_downstream")
      assertBackingState("ps_snapshot_upstream", upstream)
      assertPublicResult("ps_snapshot_downstream", downstream)
    }

    it("chains incrementally through a typed public projection and reopens its persisted layout after restart") {
      val restartConf = extraSparkConf ++ Map(
        "spark.sql.catalogImplementation" -> "hive",
        "spark.hadoop.javax.jdo.option.ConnectionURL" ->
          s"jdbc:derby:;databaseName=$warehouseDir/metastore;create=true"
      )
      restartSpark(restartConf)
      sql("CREATE TABLE ps_chain_source (id INT, bucket STRING, amount DECIMAL(10, 2)) USING DELTA")
      sql("INSERT INTO ps_chain_source VALUES (1, 'a', 2.50), (2, 'a', NULL), (3, 'b', NULL)")
      val upstream   = "SELECT bucket, SUM(amount) AS total FROM ps_chain_source GROUP BY bucket"
      val downstream = "SELECT bucket, total FROM ps_chain_upstream WHERE total IS NOT NULL"
      sql(s"CREATE MATERIALIZED VIEW default.ps_chain_upstream AS $upstream")
      sql(s"CREATE MATERIALIZED VIEW default.ps_chain_downstream AS $downstream")
      assertBackingState("ps_chain_upstream", upstream)
      metadata("ps_chain_downstream").refreshType shouldBe RefreshTypeCode.SimpleProjection
      metadata("ps_chain_downstream").sourceTables shouldBe Seq("default.ps_chain_upstream")
      assertPublicResult("ps_chain_downstream", downstream)

      val beforeRestart = metadata("ps_chain_upstream")
      val beforeVersion = DeltaTableVersion.requireLatest(spark, beforeRestart.location)
      restartSpark(restartConf)
      metadata("ps_chain_upstream").usesBackingDataTable shouldBe true
      metadata("ps_chain_upstream").location shouldBe beforeRestart.location
      assertBackingState("ps_chain_upstream", upstream)
      assertPublicResult("ps_chain_downstream", downstream)

      sql("UPDATE ps_chain_source SET amount = 7.25 WHERE id = 3")
      sql("DELETE FROM ps_chain_source WHERE id = 1")
      refreshMv("ps_chain_upstream")
      refreshMv("ps_chain_downstream")
      metadata("ps_chain_downstream").refreshType shouldBe RefreshTypeCode.SimpleProjection
      assertBackingState("ps_chain_upstream", upstream)
      assertPublicResult("ps_chain_downstream", downstream)

      val source     = "default.ps_chain_upstream"
      val endVersion = DeltaTableVersion.requireLatest(spark, metadata("ps_chain_upstream").location)
      val batch      = SourceVersionChangeBatch(source, beforeVersion, endVersion)
      SourceVersionDelta.registerSourceDeltaView(spark, batch, spark.table(source).schema)
      val signed = spark
        .table(StagingDeltaView.deltaViewName(source))
        .select("bucket", "total", "openivm_multiplicity")
      val expectedSigned = sql(
        """SELECT bucket, CAST(total AS DECIMAL(20, 2)) AS total, sign AS openivm_multiplicity
          |FROM VALUES ('a', 2.50, -1), ('b', NULL, -1), ('a', NULL, 1), ('b', 7.25, 1)
          |AS changes(bucket, total, sign)""".stripMargin
      )
      signed.exceptAll(expectedSigned).count() shouldBe 0L
      expectedSigned.exceptAll(signed).count() shouldBe 0L

    }
  }
}
