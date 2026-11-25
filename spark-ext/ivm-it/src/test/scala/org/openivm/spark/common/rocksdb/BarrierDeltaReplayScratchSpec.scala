package org.openivm.spark.common.rocksdb

import java.io.File

import org.openivm.spark.common.FeatureGate
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class BarrierDeltaReplayScratchSpec extends IvmParitySpecBase("barrier-delta-replay-scratch") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    FeatureGate.StateSyncUriKey -> new File(warehouseDir, "state-sync-remote").toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false",
    FeatureGate.CatalogBackendKey -> "delta",
    FeatureGate.CatalogPathKey -> new File(warehouseDir, "delta-catalog").toURI.toString,
    FeatureGate.GcEnabledKey -> "false"
  )

  it("does not replay applied inputs when the last published snapshot predates Delta catalog finalization") {
    sql("CREATE TABLE barrier_scratch_src(id INT, amount INT) USING DELTA")
    sql("INSERT INTO barrier_scratch_src VALUES (1, 10)")
    val query = "SELECT id, SUM(amount) AS total FROM barrier_scratch_src GROUP BY id"
    sql(s"CREATE MATERIALIZED VIEW barrier_scratch_mv AS $query")
    sql("INSERT INTO barrier_scratch_src VALUES (1, 25)")
    OpenIvmStateSync.backupNow(spark)
    val commits = new File(warehouseDir, "state-sync-remote/.openivm-state-sync/commits")
    def commitFiles: Set[File] =
      Option(commits.listFiles()).getOrElse(Array.empty[File]).filter(_.getName.endsWith(".commit")).toSet
    val before = commitFiles
    before should not be empty

    refreshMv("barrier_scratch_mv")
    assertMvCorrect("barrier_scratch_mv", query)
    val newer = commitFiles.diff(before)
    newer should not be empty
    newer.foreach(file => file.delete() shouldBe true)

    OpenIvmRocksDBRegistry.closeAll()
    deleteDir(new File(warehouseDir, "_openivm"))
    OpenIvmStateSync.resetForTesting()
    refreshMv("barrier_scratch_mv")
    assertMvCorrect("barrier_scratch_mv", query)
  }
}
