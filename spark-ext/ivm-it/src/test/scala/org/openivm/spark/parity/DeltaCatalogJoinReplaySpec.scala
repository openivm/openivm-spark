package org.openivm.spark.parity

import java.io.File

import org.openivm.spark.common.{MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}

abstract class DeltaCatalogJoinReplayScenarios extends IvmParitySpecBase("delta-catalog-join-replay") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.openivm.catalog.backend"                   -> "delta",
    "spark.openivm.catalog.path"                      -> new File(warehouseDir, "catalog").getAbsolutePath,
    "spark.openivm.statePath"                         -> new File(warehouseDir, "state").getAbsolutePath,
    "spark.openivm.stateSync.uri"                     -> new File(warehouseDir, "state-sync").toURI.toString,
    "spark.openivm.stateSync.async.enabled"           -> "false",
    "spark.openivm.profile.refresh"                  -> "true",
    "spark.openivm.queryLog.enabled"                 -> "true",
    "spark.openivm.refresh.windowRunningIncremental.enabled" -> "true",
    "spark.sql.session.timeZone"                     -> "UTC",
    "spark.sql.shuffle.partitions"                    -> "4"
  )

  it("does not replay a previous insert after updating and deleting joined rows") {
    val schema = "join_replay_lab"
    val securities = s"$schema.securities"
    val trades = s"$schema.trades"
    val mv = s"$schema.joined_trades"
    val body = s"SELECT s.symbol, SUM(t.notional) AS total FROM $securities s JOIN $trades t " +
      "ON s.symbol = t.symbol GROUP BY s.symbol"
    sql(s"CREATE DATABASE $schema")
    sql(s"CREATE TABLE $securities(symbol STRING, company STRING) USING DELTA " +
      s"LOCATION '${new File(warehouseDir, "securities").toURI}'")
    sql(s"CREATE TABLE $trades(trade_id INT, symbol STRING, notional INT) USING DELTA " +
      s"LOCATION '${new File(warehouseDir, "trades").toURI}'")
    sql(s"INSERT INTO $securities VALUES ('AAA','Alpha Co'),('BBB','Beta Co'),('CCC','Gamma Co')")
    sql(s"INSERT INTO $trades VALUES (1,'AAA',10),(2,'BBB',11),(3,'CCC',12)," +
      "(4,'AAA',13),(5,'BBB',14),(6,'CCC',15)")
    sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
    val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.AggregateGroup
    assertMvCorrect(mv, body)

    def refreshChanged(): Unit = {
      RefreshProfileCatalog.removeAll(spark)
      refreshMv(mv)
      RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
      assertMvCorrect(mv, body)
    }

    sql(s"INSERT INTO $trades VALUES (7,'AAA',25),(8,'BBB',30)")
    refreshChanged()
    sql(s"UPDATE $trades SET notional=99 WHERE trade_id=1")
    sql(s"DELETE FROM $trades WHERE trade_id=3")
    refreshChanged()
    refreshMv(mv)
    assertMvCorrect(mv, body)
  }
}

class DeltaCatalogJoinReplaySpec extends DeltaCatalogJoinReplayScenarios with InterceptMode
class DeltaCatalogJoinReplayCdfSpec extends DeltaCatalogJoinReplayScenarios with CdfMode
