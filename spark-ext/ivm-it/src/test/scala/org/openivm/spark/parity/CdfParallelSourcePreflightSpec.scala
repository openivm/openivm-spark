package org.openivm.spark.parity

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.openivm.spark.common.{
  CdfChangeAvailability,
  CdfChangeBatch,
  DeltaCommitClassifier,
  FeatureGate,
  MvCatalog,
  RefreshProfileCatalog,
  RefreshTypeCode
}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class CdfParallelSourcePreflightSpec extends IvmParitySpecBase("cdf-parallel-source-preflight") with CdfMode {

  override protected def extraSparkConf: Map[String, String] = Map(FeatureGate.ProfileRefreshKey -> "true")

  it("bounds workers and interrupts outstanding checks when the caller is cancelled") {
    val started            = new CountDownLatch(2)
    val stopped            = new CountDownLatch(2)
    val entered            = new AtomicInteger(0)
    val failure            = new AtomicReference[Throwable]()
    val preservedInterrupt = new AtomicBoolean(false)
    val caller = new Thread(() => {
      try {
        CdfChangeAvailability.mapSources(spark, Seq(1, 2, 3), parallel = true) { _ =>
          entered.incrementAndGet()
          started.countDown()
          try new CountDownLatch(1).await()
          finally stopped.countDown()
        }
      } catch {
        case error: InterruptedException =>
          failure.set(error)
          preservedInterrupt.set(Thread.currentThread().isInterrupted)
      }
    })
    caller.start()
    try {
      started.await(10, TimeUnit.SECONDS) shouldBe true
      entered.get() shouldBe 2
    } finally {
      caller.interrupt()
      caller.join(10000)
    }
    caller.isAlive shouldBe false
    stopped.await(10, TimeUnit.SECONDS) shouldBe true
    failure.get() shouldBe a[InterruptedException]
    preservedInterrupt.get() shouldBe true
  }

  it("preserves source availability results and propagates invalid source failures") {
    sql("CREATE TABLE cpsp_a (k INT, v INT) USING DELTA")
    sql("CREATE TABLE cpsp_b (k INT, v INT) USING DELTA")
    val a0 = DeltaCommitClassifier.latestVersion(spark, "cpsp_a")
    val b0 = DeltaCommitClassifier.latestVersion(spark, "cpsp_b")
    sql("INSERT INTO cpsp_a VALUES (1, 10)")
    sql("INSERT INTO cpsp_b VALUES (1, 20)")
    val checks = Seq(
      CdfChangeBatch("cpsp_a", a0, DeltaCommitClassifier.latestVersion(spark, "cpsp_a")),
      CdfChangeBatch("cpsp_b", b0, DeltaCommitClassifier.latestVersion(spark, "cpsp_b")),
      CdfChangeBatch("missing_baseline", CdfChangeBatch.NoBaseline, 1)
    )
    val expected = Map("missing_baseline" -> "cdf_watermark_missing")
    CdfChangeAvailability.unavailableReasons(spark, checks, parallel = false) shouldBe expected
    CdfChangeAvailability.unavailableReasons(spark, checks, parallel = true) shouldBe expected
    val invalid = checks :+ CdfChangeBatch("cpsp_!invalid", 0, 1)
    intercept[Exception] {
      CdfChangeAvailability.unavailableReasons(spark, invalid, parallel = false)
    }
    intercept[Exception] {
      CdfChangeAvailability.unavailableReasons(spark, invalid, parallel = true)
    }
  }

  it("keeps multi-source refresh incremental and bag-equivalent with preflight off and on") {
    sql("CREATE TABLE cpsp_join_a (k INT, v INT) USING DELTA")
    sql("CREATE TABLE cpsp_join_b (k INT, v INT) USING DELTA")
    sql("INSERT INTO cpsp_join_a VALUES (1, 10)")
    sql("INSERT INTO cpsp_join_b VALUES (1, 20)")
    val query = "SELECT a.k, a.v AS av, b.v AS bv FROM cpsp_join_a a JOIN cpsp_join_b b ON a.k = b.k"
    Seq("cpsp_mv_off", "cpsp_mv_on").foreach { mv =>
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      MvCatalog.lookup(spark, id).getOrElse(fail(s"Missing $mv")).refreshType should not be RefreshTypeCode.FullRefresh
    }
    sql("INSERT INTO cpsp_join_a VALUES (2, 30)")
    sql("INSERT INTO cpsp_join_b VALUES (2, 40)")
    val timings = scala.collection.mutable.ArrayBuffer.empty[String]
    try {
      Seq(false -> "cpsp_mv_off", true -> "cpsp_mv_on").foreach { case (enabled, mv) =>
        spark.conf.set(CdfChangeAvailability.ParallelSourcesKey, enabled.toString)
        RefreshProfileCatalog.removeAll(spark)
        refreshMv(mv)
        val profile = RefreshProfileCatalog.scanAll(spark)
        profile.map(_.detail).mkString("\n") should include("outcome=incremental_executed")
        val preflight =
          profile.filter(row => row.stepName == "metadata_pre_sql" && row.detail.contains("cdf_change_availability"))
        preflight.size shouldBe 1
        preflight.head.detail should include("source_count=2")
        timings += s"""{"parallel":$enabled,"sourceCount":2,"durationMs":${preflight.head.durationMs}}"""
        assertMvCorrect(mv, query)
      }
    } finally spark.conf.unset(CdfChangeAvailability.ParallelSourcesKey)
    sys.env.get("OPENIVM_CDF_PREFLIGHT_EVIDENCE").foreach { path =>
      Files.write(Paths.get(path), (timings.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
    }
  }

  it("keeps multi-source refresh incremental and bag-equivalent with watermark multi-get off and on") {
    sql("CREATE TABLE cpsp_wm_a (k INT, v INT) USING DELTA")
    sql("CREATE TABLE cpsp_wm_b (k INT, v INT) USING DELTA")
    sql("INSERT INTO cpsp_wm_a VALUES (1, 10), (1, 10)")
    sql("INSERT INTO cpsp_wm_b VALUES (1, 20)")
    val query = "SELECT a.k, a.v AS av, b.v AS bv FROM cpsp_wm_a a JOIN cpsp_wm_b b ON a.k = b.k"
    Seq("cpsp_wm_off", "cpsp_wm_on").foreach { mv =>
      sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
      val id = spark.sessionState.sqlParser.parseTableIdentifier(mv)
      MvCatalog.lookup(spark, id).getOrElse(fail(s"Missing $mv")).refreshType should not be RefreshTypeCode.FullRefresh
    }
    sql("INSERT INTO cpsp_wm_a VALUES (2, 30), (2, 30)")
    sql("INSERT INTO cpsp_wm_b VALUES (2, 40)")
    val flag = "spark.openivm.cdf.watermarks.multiGet.enabled"
    try {
      Seq(false -> "cpsp_wm_off", true -> "cpsp_wm_on").foreach { case (enabled, mv) =>
        spark.conf.set(flag, enabled.toString)
        RefreshProfileCatalog.removeAll(spark)
        refreshMv(mv)
        RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should include("outcome=incremental_executed")
        assertMvCorrect(mv, query)
      }
    } finally spark.conf.unset(flag)
  }
}
