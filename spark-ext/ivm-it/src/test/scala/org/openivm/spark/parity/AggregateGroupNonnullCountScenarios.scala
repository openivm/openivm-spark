package org.openivm.spark.parity

import org.apache.spark.sql.DataFrame
import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.IvmParitySpecBase

/** Native-independent oracle for the count columns that decide when an
  * AGGREGATE_GROUP row is retired. After every refresh each hidden or visible
  * count must equal its recomputed source aggregate and lie in `[0, COUNT(*)]`.
  * Under that invariant a `COUNT(*) = 0` retirement predicate and one that
  * requires every count (including `openivm_nonnull_sum_count_*`) to be zero
  * retire exactly the same groups. Names are prefixed with `aggnn_`.
  */
abstract class AggregateGroupNonnullCountScenarios extends IvmParitySpecBase("aggregate-group-nonnull-count") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  private val sumCountCounts = Map("openivm_nonnull_sum_count_1" -> "COUNT(x)", "cnt" -> "COUNT(*)")

  private def sumCountSql(src: String): String =
    s"SELECT k, SUM(x) AS total, COUNT(*) AS cnt FROM $src GROUP BY k"

  private def createMv(mv: String, viewSql: String): Unit = {
    sql(s"CREATE MATERIALIZED VIEW $mv AS $viewSql")
    val id   = spark.sessionState.sqlParser.parseTableIdentifier(mv)
    val meta = MvCatalog.lookup(spark, id).getOrElse(fail(s"$mv not in MvCatalog"))
    meta.refreshType shouldBe RefreshTypeCode.AggregateGroup
  }

  private def mvData(mv: String): DataFrame = spark.read.format("delta").load(mvDataLocation(mv))

  private def assertCounts(mv: String, src: String, counts: Map[String, String], starCol: String): Unit = {
    val data = mvData(mv)
    data.columns.toSet.filter(_.startsWith("openivm_nonnull_sum_count")) shouldBe
      counts.keySet.filter(_.startsWith("openivm_nonnull_sum_count"))
    data.createOrReplaceTempView("aggnn_mv_data")
    val cols     = counts.keys.toSeq.sorted
    val srcAgg   = cols.map(c => s"${counts(c)} AS `s_$c`").mkString(", ")
    val mismatch = cols.map(c => s"NOT (m.`$c` <=> s.`s_$c`)").mkString(" OR ")
    val bounds   = cols.map(c => s"m.`$c` < 0 OR m.`$c` > m.`$starCol`").mkString(" OR ")
    val bad = spark
      .sql(
        s"SELECT * FROM aggnn_mv_data m FULL OUTER JOIN (SELECT k, $srcAgg FROM $src GROUP BY k) s ON m.k <=> s.k " +
          s"WHERE $mismatch OR $bounds OR COALESCE(m.`$starCol`, 0) <= 0"
      )
      .collect()
    withClue(s"$mv count columns diverge from $src: ${bad.mkString("; ")}") {
      bad shouldBe empty
    }
  }

  private def refreshAndCheck(
      mv: String,
      src: String,
      viewSql: String,
      counts: Map[String, String],
      starCol: String
  ): Unit = {
    refreshMv(mv)
    assertMvCorrect(mv, viewSql)
    assertCounts(mv, src, counts, starCol)
  }

  private type NullOut = (String, String, String) => Unit

  private val viaUpdate: NullOut = (src, col, where) => sql(s"UPDATE $src SET $col = NULL WHERE $where")

  private val viaReplace: NullOut = (src, col, where) => {
    val df    = spark.table(src).where(where)
    val names = df.columns.toSeq
    val rows  = df.collect()
    sql(s"DELETE FROM $src WHERE $where")
    if (rows.nonEmpty) {
      val values = rows.map { r =>
        names.indices
          .map { i =>
            if (names(i) == col || r.isNullAt(i)) "NULL"
            else
              r.get(i) match {
                case v: String => s"'$v'"
                case v         => v.toString
              }
          }
          .mkString("(", ", ", ")")
      }
      sql(s"INSERT INTO $src VALUES ${values.mkString(", ")}")
    }
  }

  // TODO(openivm-spark): under intercept mode `UPDATE ... SET col = NULL` hits the pre-existing
  // Catalyst-binding bug documented in ProjectionScenarios (6) and FilterScenarios (2b)
  // ("Couldn't find x#NNN in [k#MMM]", reproduced on natives 57b21876 and 1b128fec). The UPDATE
  // variants therefore run only under CDF; the row-replacement variants apply the same
  // non-NULL to NULL count deltas in one batch and run in both modes.
  private def nullTransitionCase(name: String, suffix: String)(body: (String, String, NullOut) => Unit): Unit = {
    itCdf(s"$name via UPDATE")(body(s"aggnn_t${suffix}u", s"aggnn_mv${suffix}u", viaUpdate))
    it(s"$name via row replacement")(body(s"aggnn_t${suffix}r", s"aggnn_mv${suffix}r", viaReplace))
  }
  describe("(1) all-NULL SUM input") {
    it("keeps all-NULL groups with a NULL sum and a zero non-NULL count") {
      val src = "aggnn_t1"
      val mv  = "aggnn_mv1"
      sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, k STRING, x INT) USING DELTA")
      sql(s"INSERT INTO $src VALUES (1, 'a', NULL), (2, 'a', NULL), (3, 'b', 1), (4, 'b', NULL)")
      createMv(mv, sumCountSql(src))
      assertCounts(mv, src, sumCountCounts, "cnt")

      sql(s"INSERT INTO $src VALUES (5, 'a', NULL), (6, 'c', NULL), (7, 'b', 2)")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")
      mvData(mv)
        .where("k IN ('a', 'c') AND total IS NULL AND openivm_nonnull_sum_count_1 = 0 AND cnt > 0")
        .count() shouldBe 2L
    }
  }

  describe("(2) empty groups") {
    it("populates from an empty source, empties completely, and re-creates groups") {
      val src = "aggnn_t2"
      val mv  = "aggnn_mv2"
      sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, k STRING, x INT) USING DELTA")
      createMv(mv, sumCountSql(src))
      assertCounts(mv, src, sumCountCounts, "cnt")

      sql(s"INSERT INTO $src VALUES (1, 'a', NULL), (2, 'b', 5), (3, 'b', NULL)")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"DELETE FROM $src")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")
      spark.table(mv).count() shouldBe 0L

      sql(s"INSERT INTO $src VALUES (4, 'a', 7), (5, 'b', NULL)")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")
    }
  }

  describe("(3) UPDATE transitions") {
    nullTransitionCase("tracks non-NULL to NULL, NULL to non-NULL, and group-key moves", "3") { (src, mv, nullOut) =>
      sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, k STRING, x INT) USING DELTA")
      sql(s"INSERT INTO $src VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', NULL), (4, 'c', 3)")
      createMv(mv, sumCountSql(src))

      nullOut(src, "x", "k = 'a'")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"UPDATE $src SET x = 4 WHERE k = 'b'")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"UPDATE $src SET k = 'd' WHERE k = 'c'")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"UPDATE $src SET k = 'a' WHERE k = 'b'")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")
    }
  }
  describe("(4) DELETE transitions") {
    it("retires a group only when its last row, NULL or not, is deleted") {
      val src = "aggnn_t4"
      val mv  = "aggnn_mv4"
      sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, k STRING, x INT) USING DELTA")
      sql(s"INSERT INTO $src VALUES (1, 'a', 1), (2, 'a', NULL), (3, 'a', NULL), (4, 'b', NULL), (5, 'b', 2)")
      createMv(mv, sumCountSql(src))

      sql(s"DELETE FROM $src WHERE x IS NOT NULL")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"DELETE FROM $src WHERE id = 2")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"DELETE FROM $src WHERE k = 'a'")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")

      sql(s"DELETE FROM $src WHERE x IS NULL")
      refreshAndCheck(mv, src, sumCountSql(src), sumCountCounts, "cnt")
      spark.table(mv).count() shouldBe 0L
    }
  }

  describe("(5) SUM without a visible COUNT(*)") {
    nullTransitionCase("maintains hidden counts across a mixed batch and a re-created key", "5") { (src, mv, nullOut) =>
      val viewSql = s"SELECT k, SUM(x) AS total FROM $src GROUP BY k"
      val counts  = Map("openivm_nonnull_sum_count_1" -> "COUNT(x)", "openivm_count_star" -> "COUNT(*)")
      sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, k STRING, x INT) USING DELTA")
      sql(s"INSERT INTO $src VALUES (1, 'a', 1), (2, 'b', 2), (3, 'b', NULL), (4, 'c', NULL)")
      createMv(mv, viewSql)
      assertCounts(mv, src, counts, "openivm_count_star")

      sql(s"INSERT INTO $src VALUES (5, 'd', NULL), (6, 'd', NULL)")
      sql(s"DELETE FROM $src WHERE k = 'a'")
      nullOut(src, "x", "id = 2")
      sql(s"UPDATE $src SET x = 9 WHERE id = 4")
      refreshAndCheck(mv, src, viewSql, counts, "openivm_count_star")

      sql(s"INSERT INTO $src VALUES (7, 'a', NULL)")
      refreshAndCheck(mv, src, viewSql, counts, "openivm_count_star")
    }
  }

  describe("(6) several SUMs with COUNT(x) and AVG") {
    nullTransitionCase("keeps every count column exact when the SUM inputs become NULL independently", "6") {
      (src, mv, nullOut) =>
        val viewSql = s"SELECT k, SUM(x) AS sx, SUM(y) AS sy, COUNT(x) AS cx, AVG(y) AS ay FROM $src GROUP BY k"
        val counts = Map(
          "openivm_nonnull_sum_count_1" -> "COUNT(x)",
          "openivm_nonnull_sum_count_2" -> "COUNT(y)",
          "cx"                          -> "COUNT(x)",
          "openivm_count_ay"            -> "COUNT(y)",
          "openivm_count_star"          -> "COUNT(*)"
        )
        sql(s"CREATE TABLE IF NOT EXISTS $src(id INT, k STRING, x INT, y INT) USING DELTA")
        sql(s"INSERT INTO $src VALUES (1, 'a', 1, NULL), (2, 'a', NULL, 2), (3, 'b', NULL, NULL), (4, 'c', 3, 4)")
        createMv(mv, viewSql)
        assertCounts(mv, src, counts, "openivm_count_star")

        nullOut(src, "x", "id = 1")
        sql(s"INSERT INTO $src VALUES (5, 'b', NULL, NULL)")
        refreshAndCheck(mv, src, viewSql, counts, "openivm_count_star")

        nullOut(src, "y", "id = 2")
        sql(s"DELETE FROM $src WHERE id = 4")
        refreshAndCheck(mv, src, viewSql, counts, "openivm_count_star")

        sql(s"DELETE FROM $src WHERE k = 'b'")
        refreshAndCheck(mv, src, viewSql, counts, "openivm_count_star")
    }
  }
}
