package org.openivm.spark.compiler

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkExpressionCompositionSpec extends AnyFlatSpec with Matchers {
  private def compile(query: String, refreshType: String): Unit = {
    val prepared = SparkExpressionPrepass.prepare(query, caseSensitive = false)
    prepared.sql should include("__openivm_spark_expression")
    CatalystSqlParser.parsePlan(prepared.sql)
    val compiler = OpenIvmCompiler.build()
    try {
      val compiled = compiler.compile(
        CompileRequest("expression_composition", prepared.sql, Map("src" -> StructType.fromDDL("g INT, s STRING")))
      )
      compiled.refreshTypeName shouldBe refreshType
      val initial = prepared.markers.restore(compiled.initialLoadSql)
      prepared.markers.requireAllSeen()
      CatalystSqlParser.parsePlan(initial)
      initial.toLowerCase should include("lower")
      prepared.markers.resetSeen()
      val refresh = prepared.markers.restore(compiled.sql)
      prepared.markers.requireAllSeen()
      refresh should not include "__openivm_spark_expression"
    } finally compiler.close()
  }

  "Spark expression composition" should "preserve native scalar inputs and nested Spark markers" in {
    Seq("lower(reverse(s))", "lower(reverse(lower(s)))").foreach { expression =>
      compile(s"SELECT g, $expression AS v FROM src", "SIMPLE_PROJECTION")
    }
  }

  it should "leave aggregate boundaries visible to native compilation" in {
    Seq("lower(max(s))", "lower(max(reverse(lower(s))))").foreach { expression =>
      compile(s"SELECT g, $expression AS v FROM src GROUP BY g", "AGGREGATE_GROUP")
    }
  }
}
