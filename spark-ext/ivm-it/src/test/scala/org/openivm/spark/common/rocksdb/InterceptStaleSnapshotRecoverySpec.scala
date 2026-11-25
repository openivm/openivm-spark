package org.openivm.spark.common.rocksdb

import java.io.File

import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

abstract class InterceptStaleSnapshotRecoveryBase(backend: String)
    extends IvmParitySpecBase(s"intercept-stale-snapshot-$backend")
    with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.StateSyncUriKey          -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false",
    FeatureGate.CatalogBackendKey        -> backend,
    FeatureGate.CatalogPathKey           -> new File(warehouseDir, "delta-catalog").toURI.toString,
    FeatureGate.GcEnabledKey             -> "false"
  )

  private def checkRecovery(snapshotAfterInsert: Boolean): Unit = {
    val suffix = if (snapshotAfterInsert) "after" else "before"
    val source = s"barrier_${backend}_${suffix}_src"
    val mv     = s"barrier_${backend}_${suffix}_mv"
    sql(s"CREATE TABLE $source(id INT, amount INT) USING DELTA")
    sql(s"INSERT INTO $source VALUES (1, 10)")
    val query = s"SELECT id, SUM(amount) AS total FROM $source GROUP BY id"
    sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
    if (!snapshotAfterInsert) OpenIvmStateSync.backupNow(spark)
    sql(s"INSERT INTO $source VALUES (1, 25)")
    if (snapshotAfterInsert) OpenIvmStateSync.backupNow(spark)
    OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => throw new java.io.IOException("injected crash"))
    try {
      intercept[Exception](refreshMv(mv))
    } finally OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
    assertMvCorrect(mv, query)

    OpenIvmRocksDBRegistry.closeAll()
    deleteDir(new File(warehouseDir, "_openivm"))
    OpenIvmStateSync.resetForTesting()
    refreshMv(mv)
    assertMvCorrect(mv, query)
  }

  it("recovers when the last snapshot contains the staged input") {
    checkRecovery(snapshotAfterInsert = true)
  }

  it("recovers when the last snapshot predates the staged input") {
    checkRecovery(snapshotAfterInsert = false)
  }
}

class InterceptStaleSnapshotDeltaSpec extends InterceptStaleSnapshotRecoveryBase("delta") {
  it("recovers the upstream delta for a downstream materialized view") {
    val source = "barrier_delta_cascade_src"
    val parent = "barrier_delta_cascade_parent"
    val child  = "barrier_delta_cascade_child"
    val query  = s"SELECT id, SUM(amount) AS total FROM $source GROUP BY id"
    sql(s"CREATE TABLE $source(id INT, amount INT) USING DELTA")
    sql(s"INSERT INTO $source VALUES (1, 10)")
    sql(s"CREATE MATERIALIZED VIEW $parent AS $query")
    sql(s"CREATE MATERIALIZED VIEW $child AS SELECT id, total FROM $parent")
    OpenIvmStateSync.backupNow(spark)
    sql(s"INSERT INTO $source VALUES (1, 25), (2, 5)")
    OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => throw new java.io.IOException("injected crash"))
    try {
      intercept[Exception](refreshMv(parent))
    } finally OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
    OpenIvmRocksDBRegistry.closeAll()
    deleteDir(new File(warehouseDir, "_openivm"))
    OpenIvmStateSync.resetForTesting()
    refreshMv(parent)
    refreshMv(child)
    assertMvCorrect(parent, query)
    assertMvCorrect(child, query)
    refreshMv(parent)
    refreshMv(child)
    assertMvCorrect(child, query)
  }
}
class InterceptStaleSnapshotRocksSpec extends InterceptStaleSnapshotRecoveryBase("rocksdb")
