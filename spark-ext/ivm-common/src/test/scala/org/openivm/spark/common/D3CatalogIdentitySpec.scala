package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocalFileSystem, Path, RemoteIterator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, IOException}
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object D3FailingLocalFileSystem {
  private val armed = new AtomicReference[Option[(CountDownLatch, CountDownLatch)]](None)
  private val fired = new AtomicBoolean(false)
  val firedStack    = new AtomicReference[Seq[String]](Nil)

  def arm(entered: CountDownLatch, release: CountDownLatch): Unit = {
    fired.set(false)
    firedStack.set(Nil)
    armed.set(Some(entered -> release))
  }

  def disarm(): Unit = armed.set(None)

  def maybeFail(path: Path): Unit =
    armed.get().foreach { case (entered, release) =>
      if (path.toString.contains("_delta_log") && fired.compareAndSet(false, true)) {
        firedStack.set(Thread.currentThread().getStackTrace.toSeq.map(_.getClassName))
        entered.countDown()
        if (!release.await(2, TimeUnit.MINUTES)) throw new IllegalStateException("injected listing was not released")
        armed.set(None)
        throw new IOException(s"injected Delta log listing failure for $path")
      }
    }
}

class D3FailingLocalFileSystem extends LocalFileSystem {
  override def listStatus(f: Path): Array[FileStatus] = {
    D3FailingLocalFileSystem.maybeFail(f)
    super.listStatus(f)
  }

  override def listStatusIterator(f: Path): RemoteIterator[FileStatus] = {
    D3FailingLocalFileSystem.maybeFail(f)
    super.listStatusIterator(f)
  }
}

class D3CatalogIdentitySpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _
  private val root = {
    val dir = new File(s"target/test-d3-catalog-identity-${UUID.randomUUID().toString.take(8)}")
    require(dir.mkdirs() || dir.isDirectory)
    dir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-D3CatalogIdentitySpec")
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

  private def metadata(name: String, source: String): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(name),
      querySql = s"SELECT count(*) FROM $source",
      refreshType = 0,
      refreshTypeName = "AGGREGATE_GROUP",
      lastVersion = 1L,
      sourceTables = Seq(source),
      sourceSchemaFingerprint = MvCatalog.schemaFingerprint(
        Map(source -> StructType(Seq(StructField("id", IntegerType))))
      ),
      location = s"$root/data/$name",
      createdAt = new Timestamp(1700000000000L),
      properties = Map("owner" -> "openivm")
    )

  describe("D3 catalog listing identity") {
    for (freshLog <- Seq(false, true)) {
      it(s"shows another session a catalog recreated at the same path and version (fresh DeltaLog = $freshLog)") {
        val catalogPath = s"$root/recreated-$freshLog"
        val tablePath   = s"$catalogPath/mv_metadata"
        val retained    = metadata("db.mv_d3_retained", "db.d3_original")
        val removed     = metadata("db.mv_d3_removed", "db.d3_removed")
        val replacement = metadata("db.mv_d3_retained", "db.d3_replacement")
        val added       = metadata("db.mv_d3_added", "db.d3_added")
        val writer      = session(catalogPath)
        val reader      = session(catalogPath)

        within(writer)(Seq(retained, removed).foreach(MvCatalog.upsert(writer, _)))
        within(reader) {
          MvCatalog.lookup(reader, retained.name) shouldBe Some(retained)
          MvCatalog.list(reader).toSet shouldBe Set(retained, removed)
        }
        val (originalId, originalVersion) = within(writer) {
          val table = DeltaTable.forPath(writer, tablePath)
          table.detail().select("id").head().getString(0) -> table.history(1).select("version").head().getLong(0)
        }
        deleteDir(new File(tablePath))
        if (freshLog) DeltaLog.clearCache()
        within(writer)(Seq(replacement, added).foreach(MvCatalog.upsert(writer, _)))
        within(writer) {
          val table = DeltaTable.forPath(writer, tablePath)
          table.detail().select("id").head().getString(0) should not be originalId
          table.history(1).select("version").head().getLong(0) shouldBe originalVersion
        }
        within(reader) {
          MvCatalog.lookup(reader, retained.name) shouldBe Some(replacement)
          MvCatalog.lookup(reader, removed.name) shouldBe None
          MvCatalog.lookup(reader, added.name) shouldBe Some(added)
          MvCatalog.list(reader).toSet shouldBe Set(replacement, added)
        }
      }
    }

    it("shows a reader commits published through another session and another DeltaLog of the same log") {
      val catalogPath = s"$root/published"
      val tablePath   = s"$catalogPath/mv_metadata"
      val entry       = metadata("db.mv_d3_published", "db.d3_published_source")
      val options     = Map("fs.openivm.d3.identity" -> "distinct")
      val reader      = session(catalogPath)
      val writer      = session(catalogPath)
      val other       = session(catalogPath)

      within(reader) {
        MvCatalog.upsert(reader, entry)
        MvCatalog.lookup(reader, entry.name) shouldBe Some(entry)
      }
      within(writer)(MvCatalog.upsert(writer, entry.copy(lastVersion = 2L)))
      within(reader)(MvCatalog.lookup(reader, entry.name).map(_.lastVersion) shouldBe Some(2L))

      within(other) {
        DeltaTable
          .forPath(other, tablePath, options)
          .updateExpr("name = 'db.mv_d3_published'", Map("last_version" -> "3"))
      }
      val readerLog = within(reader)(DeltaLog.forTable(reader, new Path(tablePath)))
      val otherLog  = within(other)(DeltaLog.forTable(other, new Path(tablePath), options))
      readerLog should not be theSameInstanceAs(otherLog)
      within(reader) {
        MvCatalog.lookup(reader, entry.name).map(_.lastVersion) shouldBe Some(3L)
        MvCatalog.list(reader).map(_.lastVersion) shouldBe Seq(3L)
      }

      DeltaLog.clearCache()
      within(writer)(MvCatalog.upsert(writer, entry.copy(lastVersion = 4L)))
      within(reader)(MvCatalog.lookup(reader, entry.name).map(_.lastVersion) shouldBe Some(4L))
    }

    it("keeps overlapping readers of one relative catalog path on the Delta log their own session resolves") {
      val scratch    = s"target/d3-identity-${UUID.randomUUID().toString.take(8)}"
      val relative   = s"$scratch/catalog"
      val localFs    = FileSystem.getLocal(spark.sessionState.newHadoopConf())
      val originalWd = localFs.getWorkingDirectory
      val workingA   = new File(s"$root/working-a")
      require(workingA.mkdirs() || workingA.isDirectory)
      localFs.setWorkingDirectory(new Path(workingA.toURI))

      def sideSession(isB: Boolean): SparkSession =
        if (isB) session(relative, "fs.file.impl.disable.cache" -> "true") else session(relative)

      val sharedName = "db.mv_d3_identity_shared"
      val own = Map(
        false -> metadata(sharedName, "db.d3_identity_a"),
        true  -> metadata(sharedName, "db.d3_identity_b")
      )
      val only = Map(
        false -> metadata("db.mv_d3_identity_a", "db.d3_only_a"),
        true  -> metadata("db.mv_d3_identity_b", "db.d3_only_b")
      )
      val finalVersion = 8L

      try {
        for (isB <- Seq(false, true)) {
          val setup = sideSession(isB)
          within(setup)(Seq(own(isB), only(isB)).foreach(MvCatalog.upsert(setup, _)))
        }
        val a    = sideSession(isB = false)
        val b    = sideSession(isB = true)
        val logA = within(a)(DeltaLog.forTable(a, new Path(s"$relative/mv_metadata")))
        val logB = within(b)(DeltaLog.forTable(b, new Path(s"$relative/mv_metadata")))
        logA.dataPath should not be logB.dataPath
        within(a)(logA.update().metadata.id) should not be within(b)(logB.update().metadata.id)
        within(a)(MvCatalog.list(a).toSet) shouldBe Set(own(false), only(false))
        within(b)(MvCatalog.list(b).toSet) shouldBe Set(own(true), only(true))

        val published                                   = Map(false -> new AtomicLong(1L), true -> new AtomicLong(1L))
        val readersPerSide                              = 3
        val barrier                                     = new CyclicBarrier(2 * (readersPerSide + 1))
        val deadline                                    = System.nanoTime() + 4.minutes.toNanos
        val pool                                        = Executors.newFixedThreadPool(2 * (readersPerSide + 1))
        implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        try {
          val work: Seq[Future[Any]] = Seq(false, true).flatMap { isB =>
            val writer = Future {
              val writerSession = sideSession(isB)
              within(writerSession) {
                barrier.await()
                for (version <- 2L to finalVersion) {
                  MvCatalog.upsert(writerSession, own(isB).copy(lastVersion = version))
                  published(isB).set(version)
                }
              }
            }
            val readers = (1 to readersPerSide).map { _ =>
              Future {
                val readerSession = sideSession(isB)
                within(readerSession) {
                  barrier.await()
                  var last       = 0L
                  var iterations = 0
                  while ((iterations < 6 || published(isB).get() < finalVersion) && System.nanoTime() < deadline) {
                    val floor = published(isB).get()
                    val seen  = MvCatalog.lookup(readerSession, own(isB).name)
                    seen.map(_.sourceTables) shouldBe Some(own(isB).sourceTables)
                    seen.get.lastVersion should be >= floor
                    seen.get.lastVersion should be >= last
                    last = seen.get.lastVersion
                    MvCatalog.lookup(readerSession, only(!isB).name) shouldBe None
                    MvCatalog.lookup(readerSession, only(isB).name) shouldBe Some(only(isB))
                    iterations += 1
                  }
                  published(isB).get() shouldBe finalVersion
                  MvCatalog.lookup(readerSession, own(isB).name).map(_.lastVersion) shouldBe Some(finalVersion)
                }
              }
            }
            writer +: readers
          }
          Await.result(Future.sequence(work), 5.minutes)
        } finally {
          pool.shutdownNow()
          ()
        }

        for (isB <- Seq(false, true)) {
          val check = sideSession(isB)
          within(check)(MvCatalog.list(check).toSet) shouldBe Set(own(isB).copy(lastVersion = finalVersion), only(isB))
        }
      } finally {
        localFs.setWorkingDirectory(originalWd)
        deleteDir(new File(scratch))
      }
    }

    it("lets a reader queued behind a failed catalog listing list the log itself") {
      val catalogPath = s"$root/failed-listing"
      val entry       = metadata("db.mv_d3_failed_listing", "db.d3_failed_listing_source")
      val writer      = session(catalogPath)
      val reader      = session(catalogPath)
      val failing = session(
        catalogPath,
        "fs.file.impl"               -> classOf[D3FailingLocalFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true"
      )

      within(writer)(MvCatalog.upsert(writer, entry))
      within(reader)(MvCatalog.lookup(reader, entry.name) shouldBe Some(entry))
      within(writer)(MvCatalog.upsert(writer, entry.copy(lastVersion = 2L)))

      val entered                                     = new CountDownLatch(1)
      val release                                     = new CountDownLatch(1)
      val pool                                        = Executors.newFixedThreadPool(2)
      implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      D3FailingLocalFileSystem.arm(entered, release)
      try {
        val failed = Future(within(failing)(Try(MvCatalog.lookup(failing, entry.name))))
        entered.await(2, TimeUnit.MINUTES) shouldBe true
        D3FailingLocalFileSystem.firedStack.get().exists(_.contains("MvCatalog")) shouldBe true
        val queued = Future(within(reader)(MvCatalog.lookup(reader, entry.name)))
        Thread.sleep(1000L)
        queued.isCompleted shouldBe false
        release.countDown()
        Await.result(queued, 2.minutes).map(_.lastVersion) shouldBe Some(2L)
        Await.result(failed, 2.minutes).toOption.flatten.foreach(_.lastVersion shouldBe 2L)
        within(failing)(MvCatalog.lookup(failing, entry.name).map(_.lastVersion)) shouldBe Some(2L)
        within(writer)(MvCatalog.upsert(writer, entry.copy(lastVersion = 3L)))
        within(reader)(MvCatalog.lookup(reader, entry.name).map(_.lastVersion)) shouldBe Some(3L)
        within(failing)(MvCatalog.lookup(failing, entry.name).map(_.lastVersion)) shouldBe Some(3L)
      } finally {
        D3FailingLocalFileSystem.disarm()
        release.countDown()
        pool.shutdownNow()
        ()
      }
    }
  }
}
