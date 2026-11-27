package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}

/** Additive-monoid slice of the SIMPLE_AGGREGATE parity coverage (SUM, COUNT,
  * AVG, STDDEV, STDDEV_POP, VARIANCE / VAR_POP). See
  * [[SimpleAggregateMinMaxSpec]] for MIN/MAX, empty-table, NULL-handling and
  * cancellation scenarios.
  */
abstract class SimpleAggregateSumScenarios extends IvmParitySpecBase("simple-aggregate-sum") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  describe("quoted scalar aggregate aliases") {
    it("refreshes SUM and COUNT named days directly from the source") {
      sql("CREATE TABLE sa_quoted_src(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_quoted_src VALUES (1, 10), (2, 20), (3, NULL)")
      val body = "SELECT SUM(amount) AS n, COUNT(*) AS days, COUNT(amount) AS MiXeD FROM sa_quoted_src"
      sql(s"CREATE MATERIALIZED VIEW sa_quoted_mv AS $body")

      def check(): Unit = {
        val id = spark.sessionState.sqlParser.parseTableIdentifier("sa_quoted_mv")
        MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleAggregate
        assertMvCorrect("sa_quoted_mv", body)
      }

      check()
      sql("INSERT INTO sa_quoted_src VALUES (4, 30), (5, NULL)")
      refreshMv("sa_quoted_mv")
      check()
      sql("UPDATE sa_quoted_src SET amount = 15 WHERE id = 1")
      sql("DELETE FROM sa_quoted_src WHERE id = 2")
      refreshMv("sa_quoted_mv")
      check()
      refreshMv("sa_quoted_mv")
      check()
    }

    it("refreshes SUM and COUNT named days through a WINDOW_PARTITION cascade") {
      sql("CREATE TABLE sa_quoted_chain_src(id INT, grp STRING, amount INT) USING DELTA")
      sql("INSERT INTO sa_quoted_chain_src VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
      val parentBody =
        "SELECT id, grp, SUM(amount) OVER (PARTITION BY grp) AS num_records FROM sa_quoted_chain_src"
      val childBody = "SELECT SUM(num_records) AS n, COUNT(*) AS days FROM sa_quoted_parent"
      sql(s"CREATE MATERIALIZED VIEW sa_quoted_parent AS $parentBody")
      sql(s"CREATE MATERIALIZED VIEW sa_quoted_child AS $childBody")

      def check(): Unit = {
        val parentId = spark.sessionState.sqlParser.parseTableIdentifier("sa_quoted_parent")
        val childId  = spark.sessionState.sqlParser.parseTableIdentifier("sa_quoted_child")
        MvCatalog.lookup(spark, parentId).get.refreshType shouldBe RefreshTypeCode.WindowPartition
        MvCatalog.lookup(spark, childId).get.refreshType shouldBe RefreshTypeCode.SimpleAggregate
        assertMvCorrect("sa_quoted_parent", parentBody)
        assertMvCorrect(
          "sa_quoted_child",
          s"SELECT SUM(num_records) AS n, COUNT(*) AS days FROM ($parentBody) expected"
        )
      }

      check()
      sql("INSERT INTO sa_quoted_chain_src VALUES (4, 'a', 40), (5, 'c', NULL)")
      refreshMv("sa_quoted_parent")
      assertMvCorrect("sa_quoted_parent", parentBody)
      refreshMv("sa_quoted_child")
      check()
      sql("UPDATE sa_quoted_chain_src SET amount = 25 WHERE id = 2")
      sql("DELETE FROM sa_quoted_chain_src WHERE id = 3")
      refreshMv("sa_quoted_parent")
      refreshMv("sa_quoted_child")
      check()
      refreshMv("sa_quoted_parent")
      refreshMv("sa_quoted_child")
      check()
    }
  }

  // ── (1) SUM scalar aggregate ───────────────────────────────────────────────

  describe("(1) SELECT SUM(amount) AS total FROM sales") {
    it("incremental refresh correctly updates the single-row SUM aggregate") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_1(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_1 VALUES (1, 100), (2, 200), (3, 50)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_1 AS SELECT SUM(amount) AS total FROM sa_sales_1"
      )
      assertMvCorrect("mv_sa_1", "SELECT SUM(amount) AS total FROM sa_sales_1")
      sql("INSERT INTO sa_sales_1 VALUES (4, 150)")
      refreshMv("mv_sa_1")
      assertMvCorrect("mv_sa_1", "SELECT SUM(amount) AS total FROM sa_sales_1")
    }
  }

  // ── (2) COUNT(*) scalar aggregate ─────────────────────────────────────────

  describe("(2) SELECT COUNT(*) AS n FROM sales") {
    it("incremental refresh correctly updates the single-row COUNT(*) aggregate") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_2(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_2 VALUES (1, 100), (2, 200)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_2 AS SELECT COUNT(*) AS n FROM sa_sales_2"
      )
      assertMvCorrect("mv_sa_2", "SELECT COUNT(*) AS n FROM sa_sales_2")
      sql("INSERT INTO sa_sales_2 VALUES (3, 300)")
      sql("DELETE FROM sa_sales_2 WHERE id = 1")
      refreshMv("mv_sa_2")
      assertMvCorrect("mv_sa_2", "SELECT COUNT(*) AS n FROM sa_sales_2")
    }
  }

  // ── (3) COUNT(column) — counts non-NULL values only ───────────────────────

  describe("(3) SELECT COUNT(amount) AS n FROM sales") {
    it("incremental refresh correctly counts non-NULL values after DML") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_3(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_3 VALUES (1, 100), (2, NULL), (3, 50)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_3 AS SELECT COUNT(amount) AS n FROM sa_sales_3"
      )
      assertMvCorrect("mv_sa_3", "SELECT COUNT(amount) AS n FROM sa_sales_3")
      sql("INSERT INTO sa_sales_3 VALUES (4, NULL), (5, 75)")
      refreshMv("mv_sa_3")
      assertMvCorrect("mv_sa_3", "SELECT COUNT(amount) AS n FROM sa_sales_3")
    }
  }

  // ── (4) AVG scalar aggregate ───────────────────────────────────────────────

  describe("(4) SELECT AVG(amount) AS avg_amount FROM sales") {
    it("refresh stays correct after INSERT (openivm reformulates AVG as SUM/COUNT)") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_4(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_4 VALUES (1, 100), (2, 200), (3, 300)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_4 AS SELECT AVG(amount) AS avg_amount FROM sa_sales_4"
      )
      assertMvCorrect("mv_sa_4", "SELECT AVG(amount) AS avg_amount FROM sa_sales_4")
      sql("INSERT INTO sa_sales_4 VALUES (4, 400)")
      refreshMv("mv_sa_4")
      assertMvCorrect("mv_sa_4", "SELECT AVG(amount) AS avg_amount FROM sa_sales_4")
    }
  }

  // ── (5) STDDEV sample + population ────────────────────────────────────────

  describe("(5) STDDEV and STDDEV_POP scalar aggregates") {
    it("STDDEV (sample) refresh stays correct after INSERT") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_5a(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_5a VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_5a AS SELECT STDDEV(amount) AS sd FROM sa_sales_5a"
      )
      assertMvCorrect("mv_sa_5a", "SELECT STDDEV(amount) AS sd FROM sa_sales_5a")
      sql("INSERT INTO sa_sales_5a VALUES (5, 50)")
      refreshMv("mv_sa_5a")
      assertMvCorrect("mv_sa_5a", "SELECT STDDEV(amount) AS sd FROM sa_sales_5a")
    }

    it("STDDEV_POP (population) refresh stays correct after INSERT") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_5b(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_5b VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_5b AS SELECT STDDEV_POP(amount) AS sd_pop FROM sa_sales_5b"
      )
      assertMvCorrect("mv_sa_5b", "SELECT STDDEV_POP(amount) AS sd_pop FROM sa_sales_5b")
      sql("INSERT INTO sa_sales_5b VALUES (5, 50)")
      refreshMv("mv_sa_5b")
      assertMvCorrect("mv_sa_5b", "SELECT STDDEV_POP(amount) AS sd_pop FROM sa_sales_5b")
    }
  }

  // ── (6) VARIANCE and VAR_POP ──────────────────────────────────────────────

  describe("(6) VARIANCE and VAR_POP scalar aggregates") {
    it("VARIANCE (sample) and VAR_POP (population) refresh correctly after INSERT") {
      sql("CREATE TABLE IF NOT EXISTS sa_sales_6(id INT, amount INT) USING DELTA")
      sql("INSERT INTO sa_sales_6 VALUES (1, 10), (2, 20), (3, 30)")
      sql(
        "CREATE MATERIALIZED VIEW mv_sa_6 AS " +
          "SELECT VARIANCE(amount) AS var_samp, VAR_POP(amount) AS var_pop FROM sa_sales_6"
      )
      assertMvCorrect(
        "mv_sa_6",
        "SELECT VARIANCE(amount) AS var_samp, VAR_POP(amount) AS var_pop FROM sa_sales_6"
      )
      sql("INSERT INTO sa_sales_6 VALUES (4, 40)")
      refreshMv("mv_sa_6")
      assertMvCorrect(
        "mv_sa_6",
        "SELECT VARIANCE(amount) AS var_samp, VAR_POP(amount) AS var_pop FROM sa_sales_6"
      )
    }
  }
}
