package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class D3WatermarkIdentitySpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _
  private val root = {
    val dir = new File(s"target/test-d3-watermark-identity-${UUID.randomUUID().toString.take(8)}")
    require(dir.mkdirs() || dir.isDirectory)
    dir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-D3WatermarkIdentitySpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"$root/warehouse")
      .config(FeatureGate.CatalogBackendKey, "delta")
      .config(FeatureGate.CatalogPathKey, s"$root/catalog")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      deleteDir(new File(root))
    }
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def session(catalogPath: String, extra: (String, String)*): SparkSession = {
    val created = spark.newSession()
    extra.foreach { case (key, value) => created.conf.set(key, value) }
    created.conf.set(FeatureGate.CatalogPathKey, catalogPath)
    created
  }

  private def within[A](session: SparkSession)(f: => A): A = {
    SparkSession.setActiveSession(session)
    try f
    finally SparkSession.clearActiveSession()
  }

  private def get(s: SparkSession, view: String, sources: String*): Map[String, Long] =
    within(s)(CdfWatermarkCatalog.getAll(s, view, sources))

  private def put(s: SparkSession, view: String, versions: (String, Long)*): Unit =
    within(s)(CdfWatermarkCatalog.putAll(s, view, versions.toMap))

  private def identity(s: SparkSession, tablePath: String): (String, Long) =
    within(s) {
      val table = DeltaTable.forPath(s, tablePath)
      table.detail().select("id").head().getString(0) -> table.history(1).select("version").head().getLong(0)
    }

  describe("D3 CDF watermark listing identity") {
    it("creates a missing watermark table and returns only the requested view and sources") {
      val catalogPath = s"$root/missing"
      val reader      = session(catalogPath)
      val writer      = session(catalogPath)
      get(reader, "db.mv_a", "db.s1") shouldBe Map.empty
      new File(s"$catalogPath/cdf_watermarks/_delta_log").isDirectory shouldBe true
      put(writer, "db.mv_a", "db.s1" -> 1L, "db.s2" -> 2L)
      put(writer, "db.mv_b", "db.s1" -> 7L)
      get(reader, "db.mv_a", "db.s1", "db.s1", "db.s3") shouldBe Map("db.s1" -> 1L)
      get(reader, "db.mv_a", "db.s1", "db.s2") shouldBe Map("db.s1" -> 1L, "db.s2" -> 2L)
      get(reader, "db.mv_b", "db.s1", "db.s2") shouldBe Map("db.s1" -> 7L)
      get(reader, "db.mv_c", "db.s1") shouldBe Map.empty
      get(reader, "db.mv_a") shouldBe Map.empty
      put(writer, "db.mv_a", "db.s1" -> 4L)
      get(reader, "db.mv_a", "db.s1", "db.s2") shouldBe Map("db.s1" -> 4L, "db.s2" -> 2L)
    }

    for (freshLog <- Seq(false, true)) {
      it(
        s"shows another session a watermark table recreated at the same path and version (fresh DeltaLog = $freshLog)"
      ) {
        val catalogPath = s"$root/recreated-$freshLog"
        val tablePath   = s"$catalogPath/cdf_watermarks"
        val writer      = session(catalogPath)
        val reader      = session(catalogPath)
        val view        = "db.mv_recreated"

        put(writer, view, "db.kept" -> 5L, "db.removed" -> 6L)
        get(reader, view, "db.kept", "db.removed", "db.added") shouldBe Map("db.kept" -> 5L, "db.removed" -> 6L)
        val (originalId, originalVersion) = identity(writer, tablePath)
        deleteDir(new File(tablePath))
        if (freshLog) DeltaLog.clearCache()
        put(writer, view, "db.kept" -> 2L, "db.added" -> 9L)
        val (replacementId, replacementVersion) = identity(writer, tablePath)
        replacementId should not be originalId
        replacementVersion shouldBe originalVersion
        get(reader, view, "db.kept", "db.removed", "db.added") shouldBe Map("db.kept" -> 2L, "db.added" -> 9L)
      }
    }

    it("shows a reader commits published through another session and another DeltaLog of the same log") {
      val catalogPath = s"$root/published"
      val tablePath   = s"$catalogPath/cdf_watermarks"
      val view        = "db.mv_published"
      val options     = Map("fs.openivm.d3.identity" -> "distinct")
      val reader      = session(catalogPath)
      val writer      = session(catalogPath)
      val other       = session(catalogPath)

      put(reader, view, "db.src" -> 1L)
      get(reader, view, "db.src") shouldBe Map("db.src" -> 1L)
      put(writer, view, "db.src" -> 2L)
      get(reader, view, "db.src") shouldBe Map("db.src" -> 2L)

      within(other) {
        DeltaTable.forPath(other, tablePath, options).updateExpr(s"view_name = '$view'", Map("version" -> "3"))
      }
      val readerLog = within(reader)(DeltaLog.forTable(reader, new Path(tablePath)))
      val otherLog  = within(other)(DeltaLog.forTable(other, new Path(tablePath), options))
      readerLog should not be theSameInstanceAs(otherLog)
      get(reader, view, "db.src") shouldBe Map("db.src" -> 3L)

      DeltaLog.clearCache()
      put(writer, view, "db.src" -> 4L)
      get(reader, view, "db.src") shouldBe Map("db.src" -> 4L)
    }

    it("keeps overlapping readers of one relative catalog path on the Delta log their own session resolves") {
      val scratch    = s"target/d3-wm-identity-${UUID.randomUUID().toString.take(8)}"
      val relative   = s"$scratch/catalog"
      val localFs    = FileSystem.getLocal(spark.sessionState.newHadoopConf())
      val originalWd = localFs.getWorkingDirectory
      val workingA   = new File(s"$root/working-a")
      require(workingA.mkdirs() || workingA.isDirectory)
      localFs.setWorkingDirectory(new Path(workingA.toURI))

      def sideSession(isB: Boolean): SparkSession =
        if (isB) session(relative, "fs.file.impl.disable.cache" -> "true") else session(relative)

      val view         = "db.mv_wm_shared"
      val source       = "db.wm_shared_source"
      val base         = Map(false -> 100L, true -> 200L)
      val only         = Map(false -> "db.mv_wm_only_a", true -> "db.mv_wm_only_b")
      val finalVersion = 8L

      try {
        for (isB <- Seq(false, true)) {
          val setup = sideSession(isB)
          put(setup, view, source      -> (base(isB) + 1L))
          put(setup, only(isB), source -> base(isB))
        }
        val a    = sideSession(isB = false)
        val b    = sideSession(isB = true)
        val logA = within(a)(DeltaLog.forTable(a, new Path(s"$relative/cdf_watermarks")))
        val logB = within(b)(DeltaLog.forTable(b, new Path(s"$relative/cdf_watermarks")))
        logA.dataPath should not be logB.dataPath
        within(a)(logA.update().metadata.id) should not be within(b)(logB.update().metadata.id)
        get(a, view, source) shouldBe Map(source -> 101L)
        get(b, view, source) shouldBe Map(source -> 201L)

        val published                                   = Map(false -> new AtomicLong(1L), true -> new AtomicLong(1L))
        val readersPerSide                              = 3
        val barrier                                     = new CyclicBarrier(2 * readersPerSide + 1)
        val deadline                                    = System.nanoTime() + 4.minutes.toNanos
        val pool                                        = Executors.newFixedThreadPool(2 * readersPerSide + 1)
        implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        try {
          val writer = Future {
            val writers = Map(false -> sideSession(isB = false), true -> sideSession(isB = true))
            barrier.await()
            for (version <- 2L to finalVersion; isB <- Seq(false, true)) {
              put(writers(isB), view, source -> (base(isB) + version))
              published(isB).set(version)
            }
          }
          val readers = Seq(false, true).flatMap { isB =>
            (1 to readersPerSide).map { _ =>
              Future {
                val readerSession = sideSession(isB)
                barrier.await()
                var last       = 0L
                var iterations = 0
                while ((iterations < 6 || published(isB).get() < finalVersion) && System.nanoTime() < deadline) {
                  val floor = base(isB) + published(isB).get()
                  val seen  = get(readerSession, view, source)(source)
                  seen should be >= floor
                  seen should be < base(isB) + 100L
                  seen should be >= last
                  last = seen
                  get(readerSession, only(!isB), source) shouldBe Map.empty
                  get(readerSession, only(isB), source) shouldBe Map(source -> base(isB))
                  iterations += 1
                }
                published(isB).get() shouldBe finalVersion
                get(readerSession, view, source) shouldBe Map(source -> (base(isB) + finalVersion))
              }
            }
          }
          val all: Seq[Future[Any]] = writer +: readers
          Await.result(Future.sequence(all), 5.minutes)
        } finally {
          pool.shutdownNow()
          ()
        }
      } finally {
        localFs.setWorkingDirectory(originalWd)
        deleteDir(new File(scratch))
      }
    }

    it("lets a reader queued behind a failed watermark listing list the log itself") {
      val catalogPath = s"$root/failed-listing"
      val view        = "db.mv_failed_listing"
      val writer      = session(catalogPath)
      val reader      = session(catalogPath)
      val failing = session(
        catalogPath,
        "fs.file.impl"               -> classOf[D3FailingLocalFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true"
      )

      put(writer, view, "db.src" -> 1L)
      get(reader, view, "db.src") shouldBe Map("db.src" -> 1L)
      put(writer, view, "db.src" -> 2L)

      val entered                                     = new CountDownLatch(1)
      val release                                     = new CountDownLatch(1)
      val pool                                        = Executors.newFixedThreadPool(2)
      implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      D3FailingLocalFileSystem.arm(entered, release)
      try {
        val failed = Future(Try(get(failing, view, "db.src")))
        entered.await(2, TimeUnit.MINUTES) shouldBe true
        D3FailingLocalFileSystem.firedStack.get().exists(_.contains("DeltaLogRefreshes")) shouldBe true
        val queued = Future(get(reader, view, "db.src"))
        Thread.sleep(1000L)
        queued.isCompleted shouldBe false
        release.countDown()
        Await.result(queued, 2.minutes) shouldBe Map("db.src" -> 2L)
        Await.result(failed, 2.minutes).toOption.foreach(_ shouldBe Map("db.src" -> 2L))
        get(failing, view, "db.src") shouldBe Map("db.src" -> 2L)
        put(writer, view, "db.src" -> 3L)
        get(reader, view, "db.src") shouldBe Map("db.src" -> 3L)
        get(failing, view, "db.src") shouldBe Map("db.src" -> 3L)
      } finally {
        D3FailingLocalFileSystem.disarm()
        release.countDown()
        pool.shutdownNow()
        ()
      }
    }
  }
}
