package org.openivm.spark.parity

import org.openivm.spark.common.MvCatalog
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

import scala.collection.mutable.ArrayBuffer

abstract class RefreshBlastRadiusScenarios(backend: String, slug: String) extends IvmParitySpecBase(slug) {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] =
    Map("spark.openivm.catalog.backend" -> backend)

  Seq("join", "aggregate", "window").foreach { shape =>
    it(s"$backend $shape") {
      val prefix = s"blast_${backend}_${modeLabel}_$shape"
      val securities = s"${prefix}_securities"
      val trades = s"${prefix}_trades"
      val mv = s"${prefix}_mv"
      sql(s"CREATE TABLE $securities(symbol STRING, company STRING) USING DELTA")
      sql(s"CREATE TABLE $trades(trade_id INT, symbol STRING, notional INT) USING DELTA")
      sql(s"INSERT INTO $securities VALUES ('AAA','Alpha Co'),('BBB','Beta Co'),('CCC','Gamma Co')")
      sql(s"INSERT INTO $trades VALUES (1,'AAA',10),(2,'BBB',11),(3,'CCC',12),(4,'AAA',13),(5,'BBB',14),(6,'CCC',15)")
      val query = shape match {
        case "join" =>
          s"SELECT s.symbol, SUM(t.notional) AS total FROM $securities s JOIN $trades t ON s.symbol=t.symbol GROUP BY s.symbol"
        case "aggregate" =>
          s"SELECT symbol, SUM(notional) AS total, MIN(notional) AS lo, MAX(notional) AS hi FROM $trades GROUP BY symbol"
        case "window" =>
          s"SELECT trade_id, symbol, notional, SUM(notional) OVER (PARTITION BY symbol ORDER BY trade_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM $trades"
      }
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      val refreshType = MvCatalog.lookup(spark, id).getOrElse(fail(s"missing MV $mv")).refreshType
      val failures = ArrayBuffer.empty[String]
      def check(phase: String): Unit = {
        val expected = spark.sql(query)
        val cols = expected.columns.toSeq
        val actual = spark.table(mv).select(cols.head, cols.tail: _*)
        val extra = actual.exceptAll(expected).collect().toSeq
        val missing = expected.exceptAll(actual).collect().toSeq
        if (extra.nonEmpty || missing.nonEmpty)
          failures += s"$phase extra=$extra missing=$missing"
      }
      check("create")
      sql(s"INSERT INTO $trades VALUES (7,'AAA',25),(8,'BBB',30)")
      refreshMv(mv)
      check("insert")
      sql(s"UPDATE $trades SET notional=99 WHERE trade_id=1")
      sql(s"DELETE FROM $trades WHERE trade_id=3")
      refreshMv(mv)
      check("update-delete")
      refreshMv(mv)
      check("unchanged")
      withClue(s"backend=$backend mode=$modeLabel shape=$shape refreshType=$refreshType ${failures.mkString("; ")}: ") {
        failures shouldBe empty
      }
    }
  }
}

class RefreshBlastRadiusDeltaInterceptSpec extends RefreshBlastRadiusScenarios("delta", "blast-di") with InterceptMode
class RefreshBlastRadiusDeltaCdfSpec extends RefreshBlastRadiusScenarios("delta", "blast-dc") with CdfMode
class RefreshBlastRadiusRocksInterceptSpec extends RefreshBlastRadiusScenarios("rocksdb", "blast-ri") with InterceptMode
class RefreshBlastRadiusRocksCdfSpec extends RefreshBlastRadiusScenarios("rocksdb", "blast-rc") with CdfMode
