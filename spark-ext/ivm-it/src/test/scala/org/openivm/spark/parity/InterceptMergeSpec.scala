package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode, StagingCatalog, StagingDelta}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class InterceptMergeSpec extends IvmParitySpecBase("intercept-merge") with InterceptMode {
  private def fixture(table: String): Seq[(String, String)] = {
    sql(
      s"""CREATE TABLE $table (id INT, grp STRING, amount INT) USING DELTA
         |TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')""".stripMargin
    )
    sql(s"INSERT INTO $table VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, NULL, 40), (5, 'stay', 50)")
    val projection = s"SELECT id, grp, amount FROM $table"
    val aggregate  = s"SELECT grp, SUM(amount) AS total, COUNT(*) AS n FROM $table GROUP BY grp"
    val views = Seq(
      s"${table}_proj"  -> projection,
      s"${table}_agg"   -> aggregate,
      s"${table}_chain" -> aggregate
    )
    sql(s"CREATE MATERIALIZED VIEW ${table}_proj AS $projection")
    sql(s"CREATE MATERIALIZED VIEW ${table}_agg AS $aggregate")
    sql(
      s"CREATE MATERIALIZED VIEW ${table}_chain AS " +
        s"SELECT grp, SUM(amount) AS total, COUNT(*) AS n FROM ${table}_proj GROUP BY grp"
    )
    views.foreach { case (name, expected) => assertMvCorrect(name, expected) }
    views
  }

  private def refreshAndCheck(views: Seq[(String, String)]): Unit =
    views.foreach { case (name, expected) =>
      refreshMv(name)
      assertMvCorrect(name, expected)
      val id = spark.sessionState.sqlParser.parseTableIdentifier(name)
      MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
    }

  private def pending(table: String): Seq[StagingDelta] =
    StagingCatalog.collectFor(spark, s"${table}_proj", Seq(s"default.$table"))

  describe("intercepted MERGE target changes without CDF") {
    Seq(false, true).foreach { typed =>
      val label = if (typed) "typed" else "bare"
      it(s"refreshes projection, grouped aggregate and chained MVs after mixed actions with $label NULL") {
        val table      = s"imrg_${label}"
        val views      = fixture(table)
        val nullString = if (typed) "CAST(NULL AS STRING)" else "NULL"
        val nullInt    = if (typed) "CAST(NULL AS INT)" else "NULL"
        sql(s"""
          MERGE INTO $table t
          USING (SELECT * FROM VALUES
            (1, 'u', 'moved', 5),
            (2, 'd', 'ignored', 999),
            (3, 'n', $nullString, $nullInt),
            (6, 'i', 'new', 60),
            (7, 'i', $nullString, $nullInt),
            (8, 's', 'ignored', 999)
            AS source(id, action, next_grp, amount)
          ) s
          ON t.id = s.id
          WHEN MATCHED AND s.action = 'd' THEN DELETE
          WHEN MATCHED AND s.action = 'n' THEN UPDATE SET t.grp = $nullString, t.amount = $nullInt
          WHEN MATCHED AND s.action = 'u' THEN UPDATE SET t.grp = s.next_grp, t.amount = t.amount + s.amount
          WHEN NOT MATCHED AND s.action = 'i' THEN INSERT (id, grp, amount) VALUES (s.id, s.next_grp, s.amount)
        """)

        val captured = pending(table)
        refreshAndCheck(views)
        captured.map(_.opType).toSet shouldBe Set(StagingDelta.OpTypes.Delete, StagingDelta.OpTypes.Insert)
        val ledger = StagingCatalog.ledger(spark, s"default.$table")
        refreshAndCheck(views)
        pending(table) shouldBe empty
        StagingCatalog.ledger(spark, s"default.$table") shouldBe ledger
      }
    }

    it("preserves queued INSERT, UPDATE and DELETE changes together with MERGE before one refresh") {
      val table = "imrg_pending"
      val views = fixture(table)
      sql(s"INSERT INTO $table VALUES (8, 'queued', 80)")
      sql(s"UPDATE $table SET grp = 'moved', amount = amount + 5 WHERE id = 1")
      sql(s"DELETE FROM $table WHERE id = 2")
      sql(s"""
        MERGE INTO $table t
        USING (SELECT * FROM VALUES (3, 'moved', 7), (4, 'delete', -1), (6, 'new', 60)
          AS changes(id, grp, amount)) s ON t.id = s.id
        WHEN MATCHED AND s.amount < 0 THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.grp = s.grp, t.amount = t.amount + s.amount
        WHEN NOT MATCHED THEN INSERT (id, grp, amount) VALUES (s.id, s.grp, s.amount)
      """)

      val differences = views.map { case (name, query) =>
        refreshMv(name)
        val expected = spark.sql(query)
        val actual   = spark.table(name).selectExpr(expected.columns.toIndexedSeq: _*)
        val extra    = actual.exceptAll(expected).count()
        val missing  = expected.exceptAll(actual).count()
        val id       = spark.sessionState.sqlParser.parseTableIdentifier(name)
        MvCatalog.lookup(spark, id).get.refreshType should not be RefreshTypeCode.FullRefresh
        (name, extra, missing)
      }
      withClue(s"MV bag differences (name, extra, missing): ${differences.mkString(", ")}: ") {
        differences.foreach { case (_, extra, missing) => (extra, missing) shouldBe ((0L, 0L)) }
      }
      pending(table) shouldBe empty
    }

    it("captures target-only actions and publishes nothing for a subsequent no-op MERGE") {
      val table = "imrg_target"
      val views = fixture(table)
      sql(s"""
        MERGE INTO $table t USING (SELECT col1 AS id FROM VALUES (1), (2), (5)) s ON t.id = s.id
        WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN UPDATE SET t.grp = NULL, t.amount = t.amount + 7
        WHEN NOT MATCHED BY SOURCE AND t.id = 4 THEN DELETE
      """)
      refreshAndCheck(views)
      pending(table) shouldBe empty
      sql(s"""
        MERGE INTO $table t USING (SELECT 1 AS id) s ON t.id = s.id
        WHEN MATCHED AND t.amount < 0 THEN DELETE
      """)
      pending(table) shouldBe empty
      refreshAndCheck(views)
    }
  }
}
