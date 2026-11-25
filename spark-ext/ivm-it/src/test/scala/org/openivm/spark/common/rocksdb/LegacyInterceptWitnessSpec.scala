package org.openivm.spark.common.rocksdb

import java.io.File

import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, StagingCatalog}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class LegacyInterceptWitnessSpec extends IvmParitySpecBase("legacy-intercept-witness") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.CatalogBackendKey -> "delta",
    FeatureGate.CatalogPathKey    -> new File(warehouseDir, "delta-catalog").toURI.toString,
    FeatureGate.ProfileRefreshKey -> "true",
    FeatureGate.GcEnabledKey      -> "false"
  )

  it("bootstraps an existing view without changing its incremental refresh path") {
    val source = "legacy_witness_src"
    val mv     = "legacy_witness_mv"
    val query  = s"SELECT id, SUM(amount) AS total FROM $source GROUP BY id"
    val id     = spark.sessionState.sqlParser.parseTableIdentifier(mv)

    sql(s"CREATE TABLE $source(id INT, amount INT) USING DELTA")
    sql(s"INSERT INTO $source VALUES (1, 10)")
    sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
    MvCatalog.lookup(spark, id).get.properties.contains(StagingCatalog.CompletedRefreshTokenKey) shouldBe false

    spark.conf.set(FeatureGate.StateSyncUriKey, new File(warehouseDir, "state-sync-remote").toURI.toString)
    spark.conf.set(FeatureGate.StateSyncAsyncEnabledKey, "false")
    sql(s"INSERT INTO $source VALUES (1, 25)")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv(s"default.$mv")
    val profile = RefreshProfileCatalog.scanAll(spark).map(row => s"${row.stepName}:${row.detail}").mkString("\n")
    profile should include("outcome=incremental_executed")
    assertMvCorrect(mv, query)
    val meta = MvCatalog.lookup(spark, id).get
    meta.properties.contains(StagingCatalog.CompletedRefreshTokenKey) shouldBe true

    sql(s"INSERT INTO $source VALUES (1, 5)")
    RefreshProfileCatalog.removeAll(spark)
    refreshMv(mv)
    RefreshProfileCatalog.scanAll(spark).map(row => s"${row.stepName}:${row.detail}").mkString("\n") should
      include("outcome=incremental_executed")
    assertMvCorrect(mv, query)
    sql(s"INSERT INTO $source VALUES (2, 5)")
    OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => throw new java.io.IOException("injected crash"))
    try {
      intercept[Exception](refreshMv(mv))
    } finally OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
    OpenIvmRocksDBRegistry.closeAll()
    deleteDir(new File(warehouseDir, "_openivm"))
    OpenIvmStateSync.resetForTesting()
    refreshMv(mv)
    assertMvCorrect(mv, query)
  }
}
