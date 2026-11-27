package org.openivm.spark.parity

import org.openivm.spark.common.{MvCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class SparkExpressionCompositionScenarios extends IvmParitySpecBase("expression-composition") {
  self: IvmParityMode =>

  private def assertIncremental(mv: String, expected: Int): Unit = {
    val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
    MvCatalog.lookup(spark, id).get.refreshType shouldBe expected
  }

  describe("Composed Spark expression markers") {
    it("incrementally maintains native scalars nested between Spark expressions") {
      sql("CREATE TABLE expr_comp_scalar_source (id INT, s STRING) USING DELTA")
      sql("INSERT INTO expr_comp_scalar_source VALUES (1, 'ÄBC'), (2, NULL), (3, 'ABC')")
      val query = """SELECT id, lower(reverse(s)) AS v, lower(reverse(lower(s))) AS nested
        |FROM expr_comp_scalar_source""".stripMargin
      val mv = "expr_comp_scalar_mv"
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      assertIncremental(mv, RefreshTypeCode.SimpleProjection)
      assertMvCorrect(mv, query)
      sql("UPDATE expr_comp_scalar_source SET s = 'ÖXY' WHERE id = 1")
      sql("DELETE FROM expr_comp_scalar_source WHERE id = 3")
      sql("INSERT INTO expr_comp_scalar_source VALUES (4, 'ÅPQ')")
      refreshMv(mv)
      assertIncremental(mv, RefreshTypeCode.SimpleProjection)
      assertMvCorrect(mv, query)
    }

    it("incrementally maintains Spark expressions around native aggregates") {
      sql("CREATE TABLE expr_comp_aggregate_source (id INT, g INT, s STRING) USING DELTA")
      sql("INSERT INTO expr_comp_aggregate_source VALUES (1, 1, 'ABC'), (2, 1, 'XYZ'), (3, 2, NULL)")
      val query = """SELECT g, lower(max(s)) AS v, lower(max(reverse(lower(s)))) AS nested
        |FROM expr_comp_aggregate_source GROUP BY g""".stripMargin
      val mv = "expr_comp_aggregate_mv"
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      assertIncremental(mv, RefreshTypeCode.AggregateGroup)
      assertMvCorrect(mv, query)
      sql("DELETE FROM expr_comp_aggregate_source WHERE id = 2")
      sql("UPDATE expr_comp_aggregate_source SET s = 'ÖXY' WHERE id = 3")
      sql("INSERT INTO expr_comp_aggregate_source VALUES (4, 1, 'ÅPQ'), (5, 3, NULL)")
      refreshMv(mv)
      assertIncremental(mv, RefreshTypeCode.AggregateGroup)
      assertMvCorrect(mv, query)
      sql("DELETE FROM expr_comp_aggregate_source WHERE g = 1")
      refreshMv(mv)
      assertIncremental(mv, RefreshTypeCode.AggregateGroup)
      assertMvCorrect(mv, query)
    }
  }
}

class SparkExpressionCompositionCdfSpec       extends SparkExpressionCompositionScenarios with CdfMode
class SparkExpressionCompositionInterceptSpec extends SparkExpressionCompositionScenarios with InterceptMode
