package org.openivm.spark.parity

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.{DeltaTableVersion, MvCatalog, RefreshSqlLogCatalog, RewriteLineage}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Generic coverage for runtime selection of OpenIVM's insert-only aggregate
  * compile facts. Query-log assertions verify the executed SQL program in
  * addition to the usual full bag-equality checks.
  */
abstract class AggregateInsertOnlyCompilationScenarios extends IvmParitySpecBase("aggregate-insert-only-compilation") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    super.extraSparkConf ++ Map("spark.openivm.queryLog.enabled" -> "true", "spark.openivm.profile.refresh" -> "true")

  private def rewrittenSql(view: String): Seq[String] =
    sql("SHOW OPENIVM QUERY LOG")
      .collect()
      .toSeq
      .filter(_.getString(1).split("\\.").last == view)
      .filter(_.getString(6) == "rewritten_stmt")
      .map(_.getString(9))

  describe("terminal insert-only grouped aggregates") {
    itCdf("executes one direct MERGE for SUM, COUNT, MIN, and MAX") {
      sql("CREATE TABLE aio_events (grp INT, value INT, event_ts TIMESTAMP) USING DELTA")
      sql(
        "INSERT INTO aio_events VALUES " +
          "(1, 10, TIMESTAMP'2026-01-01 10:00:00'), " +
          "(2, 20, TIMESTAMP'2026-01-01 11:00:00')"
      )
      val viewBody =
        "SELECT grp, SUM(value) AS total, COUNT(*) AS row_count, " +
          "MIN(event_ts) AS first_at, MAX(event_ts) AS last_at " +
          "FROM aio_events GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aio_rollup AS $viewBody")

      RefreshSqlLogCatalog.removeAll(spark)
      sql(
        "INSERT INTO aio_events VALUES " +
          "(1, 7, TIMESTAMP'2025-12-31 09:00:00'), " +
          "(1, 8, TIMESTAMP'2026-01-03 12:00:00'), " +
          "(3, 30, TIMESTAMP'2026-01-02 08:00:00')"
      )
      refreshMv("aio_rollup")
      assertMvCorrect("aio_rollup", viewBody)

      val statements = rewrittenSql("aio_rollup")
      statements should have size 1
      statements.head.toUpperCase should include("MERGE INTO")
      statements.head.toUpperCase should not include "CREATE OR REPLACE TABLE"
      statements.head should include("__openivm_direct_delta")
      val attempts = sql("SHOW OPENIVM REFRESH PROFILE")
        .collect()
        .toSeq
        .filter(row => row.getString(1).contains("aio_rollup") && row.getString(4) == "execute_refresh_sql_attempt")
        .map(_.getString(6))
        .filter(_.contains("source_output_indices=0,1"))
      attempts should have size 1
      val fields = attempts.head
        .split(";")
        .iterator
        .map(_.split("=", 2))
        .collect { case Array(key, value) =>
          key -> value
        }
        .toMap
      fields("source_ordinals").split(",") should have length 2
      fields("source_digests").split(",").foreach(_ should fullyMatch regex "[0-9a-f]{64}")
      fields("source_rewrite_rules").split(",") should have length 2
      fields("rewrite_pipeline") should include("inline-view-delta-ctas-into-merge@1")
      fields("executed_digest") shouldBe RewriteLineage.sha256Utf8(statements.head)
      fields("origin_class") shouldBe "unknown"
      fields("credit_ns") shouldBe "0"
    }

    itCdf("executes one direct MERGE for MIN/MAX with a derived projection") {
      sql("CREATE TABLE aio_status_events (grp INT, action STRING, event_ts TIMESTAMP) USING DELTA")
      sql("INSERT INTO aio_status_events VALUES (1, 'open', TIMESTAMP'2026-01-01 10:00:00')")
      val viewBody =
        "WITH grouped AS (" +
          "SELECT grp, MIN(CASE WHEN action = 'open' THEN event_ts END) AS opened_at, " +
          "MAX(CASE WHEN action = 'close' THEN event_ts END) AS closed_at " +
          "FROM aio_status_events GROUP BY grp) " +
          "SELECT *, CASE WHEN closed_at IS NULL THEN 'open' ELSE 'closed' END AS status FROM grouped"
      sql(s"CREATE MATERIALIZED VIEW aio_status_rollup AS $viewBody")

      RefreshSqlLogCatalog.removeAll(spark)
      sql(
        "INSERT INTO aio_status_events VALUES " +
          "(1, 'close', TIMESTAMP'2026-01-02 10:00:00'), " +
          "(2, 'open', TIMESTAMP'2026-01-02 11:00:00')"
      )
      refreshMv("aio_status_rollup")
      assertMvCorrect("aio_status_rollup", viewBody)

      val statements = rewrittenSql("aio_status_rollup")
      statements should have size 1
      statements.head should include("__openivm_direct_delta")
    }

    itCdf("falls back to the general program for one mixed DML batch") {
      sql("CREATE TABLE aio_mixed_events (id INT, grp INT, value INT, event_ts TIMESTAMP) USING DELTA")
      sql(
        "INSERT INTO aio_mixed_events VALUES " +
          "(1, 1, 10, TIMESTAMP'2026-01-01 10:00:00'), " +
          "(2, 2, 20, TIMESTAMP'2026-01-01 11:00:00')"
      )
      val viewBody =
        "SELECT grp, SUM(value) AS total, COUNT(*) AS row_count, " +
          "MIN(event_ts) AS first_at, MAX(event_ts) AS last_at " +
          "FROM aio_mixed_events GROUP BY grp"
      sql(s"CREATE MATERIALIZED VIEW aio_mixed_rollup AS $viewBody")

      RefreshSqlLogCatalog.removeAll(spark)
      sql("INSERT INTO aio_mixed_events VALUES (3, 3, 40, TIMESTAMP'2026-02-01 10:00:00')")
      sql("UPDATE aio_mixed_events SET value = 11 WHERE id = 1")
      sql("DELETE FROM aio_mixed_events WHERE id = 2")
      refreshMv("aio_mixed_rollup")

      assertMvCorrect("aio_mixed_rollup", viewBody)
      rewrittenSql("aio_mixed_rollup").size should be > 1
    }

    itCdf("does not apply the direct MERGE to an aggregate over a join") {
      sql("CREATE TABLE aio_join_facts (id INT, grp INT, value INT) USING DELTA")
      sql("CREATE TABLE aio_join_labels (id INT, label STRING) USING DELTA")
      sql("INSERT INTO aio_join_facts VALUES (1, 1, 10), (2, 2, 20)")
      val viewBody =
        "SELECT f.grp, SUM(f.value) AS total, COUNT(*) AS row_count " +
          "FROM aio_join_facts f LEFT JOIN aio_join_labels l ON f.id = l.id GROUP BY f.grp"
      sql(s"CREATE MATERIALIZED VIEW aio_join_rollup AS $viewBody")

      RefreshSqlLogCatalog.removeAll(spark)
      sql("INSERT INTO aio_join_labels VALUES (1, 'one')")
      refreshMv("aio_join_rollup")
      assertMvCorrect("aio_join_rollup", viewBody)

      val statements = rewrittenSql("aio_join_rollup")
      statements.size should be > 1
      statements.mkString("\n") should not include "__openivm_direct_delta"
    }
  }

  describe("downstream identity") {
    itCdf("uses one direct MERGE and publishes both images to a real downstream consumer") {
      sql("CREATE TABLE aio_real_cascade_source (grp INT, value INT) USING DELTA")
      sql("INSERT INTO aio_real_cascade_source VALUES (1, 10), (2, 20), (4, 40)")
      val upstreamBody   = "SELECT grp, SUM(value) AS total FROM aio_real_cascade_source GROUP BY grp"
      val downstreamBody = "SELECT grp, total FROM aio_real_cascade_rollup"
      sql(s"CREATE MATERIALIZED VIEW aio_real_cascade_rollup AS $upstreamBody")
      val log               = DeltaLog.forTable(spark, new Path(mvDataLocation("aio_real_cascade_rollup")))
      def liveDvFiles: Long = log.update().allFiles.filter("deletionVector IS NOT NULL").count()

      sql("UPDATE aio_real_cascade_source SET value = 21 WHERE grp = 2")
      refreshMv("aio_real_cascade_rollup")
      liveDvFiles shouldBe 1L
      sql(s"CREATE MATERIALIZED VIEW aio_rollup_consumer AS $downstreamBody")

      RefreshSqlLogCatalog.removeAll(spark)
      sql("INSERT INTO aio_real_cascade_source VALUES (1, 2), (3, 30)")
      refreshMv("aio_real_cascade_rollup")
      liveDvFiles shouldBe 0L
      refreshMv("aio_rollup_consumer")
      assertMvCorrect("aio_real_cascade_rollup", upstreamBody)
      assertMvCorrect("aio_rollup_consumer", downstreamBody)

      val cascadingSql = rewrittenSql("aio_real_cascade_rollup").map(_.toUpperCase)
      cascadingSql should have size 1
      cascadingSql.head should include("MERGE INTO")
      cascadingSql.head should not include "CREATE OR REPLACE TABLE"
      cascadingSql.head should not include "WHEN MATCHED THEN DELETE"

      val id       = spark.sessionState.sqlParser.parseTableIdentifier("aio_real_cascade_rollup")
      val location = MvCatalog.lookup(spark, id).getOrElse(fail("missing rollup metadata")).location
      val version  = DeltaTableVersion.requireLatest(spark, location)
      val changes = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", version)
        .option("endingVersion", version)
        .load(location)
        .select("grp", "total", "_change_type")
      val expected = sql(
        """SELECT * FROM VALUES
          |  (1, CAST(10 AS BIGINT), 'update_preimage'),
          |  (1, CAST(12 AS BIGINT), 'update_postimage'),
          |  (3, CAST(30 AS BIGINT), 'insert')
          |AS changes(grp, total, _change_type)""".stripMargin
      )
      changes.exceptAll(expected).count() shouldBe 0L
      expected.exceptAll(changes).count() shouldBe 0L
    }
  }
}
