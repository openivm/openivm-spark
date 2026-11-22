package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Paths
import java.sql.Timestamp
import java.util.concurrent.CyclicBarrier
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class StagingCatalogSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private val MvDbColumnFamilies = OpenIvmStatePaths.PerMvColumnFamilies

  private var spark: SparkSession = _

  private val warehouseDir: String = {
    val d = new File(s"target/test-warehouse-staging-${UUID.randomUUID().toString.take(8)}")
    d.mkdirs()
    d.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-StagingCatalogSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      OpenIvmRocksDBRegistry.closeAll()
      deleteDir(new File(warehouseDir))
    }
  }

  private def deleteDir(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir))
    f.delete()
    ()
  }

  private def ts(epochMs: Long): Timestamp = new Timestamp(epochMs)

  private def delta(
      baseTable: String,
      opType: String,
      path: String,
      epochMs: Long,
      consumedBy: Seq[String] = Seq.empty
  ): StagingDelta =
    StagingDelta(baseTable, opType, path, ts(epochMs), consumedBy)

  private def trackedMvDbPath(viewName: String): String =
    Paths
      .get(warehouseDir, "_openivm", "mvs", RocksDBCodec.safePathSegment(viewName), "rocksdb")
      .toString

  private def openTrackedMvDb(viewName: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(spark, trackedMvDbPath(viewName), MvDbColumnFamilies)

  private def registerTrackedView(viewName: String): Unit = {
    openTrackedMvDb(viewName)
    ()
  }

  private def consumedPaths(viewName: String): Seq[String] =
    openTrackedMvDb(viewName)
      .prefixScan("consumed", Array.emptyByteArray)
      .map { case (key, _) => RocksDBCodec.fromUtf8(key) }
      .toVector

  describe("StagingCatalog.ensureTables") {
    it("is idempotent — calling twice does not throw") {
      StagingCatalog.ensureTables(spark)
      StagingCatalog.ensureTables(spark)
    }
  }

  describe("StagingCatalog.record + collectFor") {
    it("returns only unconsumed deltas for the given sources, ordered by txn_ts") {
      StagingCatalog.ensureTables(spark)
      registerTrackedView("mv_a")

      val ordersBase   = "orders_record_collect"
      val productsBase = "products_record_collect"
      val d1           = delta(ordersBase, "INSERT", s"$warehouseDir/stg/orders/ins/1", 1000L)
      val d2           = delta(ordersBase, "DELETE", s"$warehouseDir/stg/orders/del/2", 2000L)
      val d3           = delta(productsBase, "INSERT", s"$warehouseDir/stg/products/ins/3", 500L)
      val d4           = delta(ordersBase, "INSERT", s"$warehouseDir/stg/orders/ins/4", 800L)

      Seq(d1, d2, d3, d4).foreach(StagingCatalog.record(spark, _))
      StagingCatalog.markConsumed(spark, "mv_a", Seq(d4.stagingPath))

      val result = StagingCatalog.collectFor(spark, "mv_a", Seq(ordersBase))
      result.map(_.stagingPath) shouldBe Seq(d1.stagingPath, d2.stagingPath)
      result.map(_.txnTs.getTime) shouldBe Seq(1000L, 2000L)

      val watermarks = StagingCatalog
        .currentWatermarks(spark, Seq(ordersBase, productsBase, "unused_base_table"))
        .map { case (table, timestamp) => table -> timestamp.getTime }
      watermarks shouldBe Map(ordersBase -> 2000L, productsBase -> 500L)
    }
  }

  describe("StagingCatalog.markConsumed") {
    it("creates missing consumption state and clears only its markers") {
      val viewName  = "mv_missing_consumption"
      val baseTable = "orders_missing_consumption"
      val d         = delta(baseTable, "INSERT", s"$warehouseDir/stg/orders/ins/missing", 2500L)
      val session   = spark.newSession()
      session.conf.set("spark.openivm.catalog.backend", "delta")
      session.conf.set("spark.openivm.catalog.path", s"$warehouseDir/delta-catalog")
      val name = TableIdentifier(viewName)
      val meta =
        MvMetadata(name, "SELECT 1", 0, "SIMPLE_PROJECTION", 0L, Seq(baseTable), "", warehouseDir, ts(0L), Map.empty)
      StagingCatalog.record(session, d)
      intercept[IllegalStateException] {
        StagingCatalog.markConsumed(session, viewName, Seq(d.stagingPath))
      }
      java.nio.file.Files.exists(Paths.get(trackedMvDbPath(viewName)).resolve("CURRENT")) shouldBe false
      MvCatalog.upsert(session, meta)
      StagingCatalog.markConsumed(session, viewName, Seq(d.stagingPath))
      StagingCatalog.collectFor(session, viewName, Seq(baseTable)) shouldBe empty

      val db  = openTrackedMvDb(viewName)
      val key = RocksDBCodec.utf8("unrelated")
      db.withBatch(batch => OpenIvmRocksDBBatchOps.put(db, batch, "properties", key, RocksDBCodec.utf8("keep")))
      DeltaMvCatalogBackend.remove(session, name)
      intercept[IllegalStateException] {
        StagingCatalog.markConsumed(session, viewName, Seq(d.stagingPath))
      }
      MvCatalog.prepareCreate(session, name)
      StagingCatalog.collectFor(session, viewName, Seq(baseTable)).map(_.stagingPath) shouldBe Seq(d.stagingPath)
      db.get("properties", key).map(RocksDBCodec.fromUtf8) shouldBe Some("keep")
      MvCatalog.upsert(session, meta)
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val start                                       = new CyclicBarrier(2)
      val consume = Future {
        start.await()
        try StagingCatalog.markConsumed(session, viewName, Seq(d.stagingPath))
        catch {
          case e: IllegalStateException if e.getMessage.contains("untracked materialized view") => ()
        }
      }
      val remove = Future {
        start.await()
        MvCatalog.remove(session, name)
      }
      Await.result(Future.sequence(Seq(consume, remove)), 60.seconds)
      StagingCatalog.collectFor(session, viewName, Seq(baseTable)).map(_.stagingPath) shouldBe Seq(d.stagingPath)
    }

    it("records per-MV consumed paths idempotently") {
      StagingCatalog.ensureTables(spark)
      registerTrackedView("mv_x")

      val baseTable = "orders_mark_consumed"
      val d         = delta(baseTable, "INSERT", s"$warehouseDir/stg/orders/ins/mc", 3000L)
      StagingCatalog.record(spark, d)

      StagingCatalog.markConsumed(spark, "mv_x", Seq(d.stagingPath))
      StagingCatalog.markConsumed(spark, "mv_x", Seq(d.stagingPath))

      val remaining = StagingCatalog.collectFor(spark, "mv_x", Seq(baseTable))
      remaining.filter(_.stagingPath == d.stagingPath) shouldBe empty

      val consumed = consumedPaths("mv_x")
      consumed.count(_ == d.stagingPath) shouldBe 1
    }
  }

  describe("StagingCatalog.pruneFullyConsumed") {
    it("deletes rows consumed by all dependent MVs; leaves partially-consumed rows") {
      StagingCatalog.ensureTables(spark)
      registerTrackedView("mv_p")
      registerTrackedView("mv_q")

      val baseTable = "orders_prune"
      val dFull     = delta(baseTable, "INSERT", s"$warehouseDir/stg/orders/ins/full", 5000L)
      val dPartial  = delta(baseTable, "INSERT", s"$warehouseDir/stg/orders/ins/partial", 6000L)
      val dNone     = delta(baseTable, "INSERT", s"$warehouseDir/stg/orders/ins/none", 7000L)

      Seq(dFull, dPartial, dNone).foreach(StagingCatalog.record(spark, _))

      StagingCatalog.markConsumed(spark, "mv_p", Seq(dFull.stagingPath, dPartial.stagingPath))
      StagingCatalog.markConsumed(spark, "mv_q", Seq(dFull.stagingPath))
      StagingCatalog.pruneFullyConsumed(spark, Map(baseTable -> Seq("mv_p", "mv_q")))

      val remaining =
        StagingCatalog.collectFor(spark, "__probe_remaining__", Seq(baseTable)).map(_.stagingPath).toSet

      remaining shouldBe Set(dPartial.stagingPath, dNone.stagingPath)
    }
  }

  describe("StagingCatalog concurrent MV consumers") {
    it("keeps per-MV consumed markers isolated under synchronized starts") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      StagingCatalog.ensureTables(spark)
      val leftMv    = "mv_concurrent_left"
      val rightMv   = "mv_concurrent_right"
      val baseTable = "orders_concurrent_consumers"
      val barrier   = new CyclicBarrier(2)

      registerTrackedView(leftMv)
      registerTrackedView(rightMv)

      val staged = delta(baseTable, "INSERT", s"$warehouseDir/stg/orders/ins/concurrent", 9000L)
      StagingCatalog.record(spark, staged)

      Await.result(
        Future.sequence(
          Seq(leftMv, rightMv).map { mvName =>
            Future {
              barrier.await()
              StagingCatalog.markConsumed(spark, mvName, Seq(staged.stagingPath))
            }
          }
        ),
        30.seconds
      )

      consumedPaths(leftMv) shouldBe Seq(staged.stagingPath)
      consumedPaths(rightMv) shouldBe Seq(staged.stagingPath)

      StagingCatalog.pruneFullyConsumed(spark, Map(baseTable -> Seq(leftMv, rightMv)))

      StagingCatalog.collectFor(spark, "__probe_concurrent__", Seq(baseTable)) shouldBe empty
    }
  }
}
