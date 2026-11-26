package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.util.concurrent.{CountDownLatch, CyclicBarrier, TimeUnit}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{blocking, Await, ExecutionContext, Future}

class DeltaMvCatalogBackendSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _
  private val root = {
    val dir = new File(s"target/test-delta-mv-catalog-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-DeltaMvCatalogBackendSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"$root/warehouse")
      .config(FeatureGate.CatalogBackendKey, "delta")
      .config(FeatureGate.CatalogPathKey, s"$root/catalog")
      .config("spark.ui.enabled", "false")
      .withExtensions { extensions =>
        extensions.injectCheckRule { session => plan =>
          if (session.conf.getOption("spark.openivm.test.rejectCreateTable").contains("true")) {
            plan.foreach { node =>
              if (node.nodeName.contains("CreateTable"))
                throw new IllegalStateException("CREATE TABLE is unavailable for path-backed catalogs")
            }
          }
        }
      }
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

  private def withNewSession[A](f: SparkSession => A): A = {
    val session = spark.newSession()
    SparkSession.setActiveSession(session)
    try f(session)
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

  describe("Delta MV catalog backend") {
    it("recreates an initialized catalog path after it is deleted") {
      val path   = s"$root/recreated-catalog"
      val schema = StructType(Seq(StructField("key", IntegerType), StructField("value", IntegerType)))

      DeltaPathTable.createIfAbsent(spark, path, schema, "key")
      val initialId = DeltaTable.forPath(spark, path).detail().select("id").head().getString(0)
      deleteDir(new File(path))
      DeltaPathTable.createIfAbsent(spark, path, schema, "key")

      val replacement = DeltaTable.forPath(spark, path).detail().head()
      replacement.getAs[String]("id") should not be initialId
      replacement.getAs[Seq[String]]("partitionColumns") shouldBe Seq("key")
    }

    it("does not mistake an ancestor Delta table for the requested catalog path") {
      val parent = s"$root/parent-catalog"
      val child  = s"$parent/child-catalog"
      val schema = StructType(Seq(StructField("key", IntegerType), StructField("value", IntegerType)))

      DeltaPathTable.createIfAbsent(spark, parent, schema, "key")
      DeltaPathTable.createIfAbsent(spark, child, schema, "key")

      new File(s"$child/_delta_log").isDirectory shouldBe true
    }

    it("initializes both catalog paths concurrently and preserves data when reinitialized from another schema") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val catalogPath                                 = s"$root/concurrent-initialization"
      val barrier                                     = new CyclicBarrier(2)
      spark.sql("CREATE DATABASE IF NOT EXISTS delta_init_context")

      def inContext[A](f: SparkSession => A): A = withNewSession { session =>
        session.conf.set(FeatureGate.CatalogPathKey, catalogPath)
        session.catalog.setCurrentDatabase("delta_init_context")
        session.conf.set("spark.openivm.test.rejectCreateTable", "true")
        f(session)
      }

      Await.result(
        Future.sequence(
          (1 to 2).map(_ =>
            Future {
              inContext { session =>
                barrier.await()
                MvCatalog.ensureTables(session)
                CdfWatermarkCatalog.ensureTables(session)
              }
            }
          )
        ),
        5.minutes
      )

      inContext { session =>
        val entry = metadata("db.mv_init", "db.source_init")
        MvCatalog.upsert(session, entry)
        CdfWatermarkCatalog.put(session, "db.mv_init", "db.source_init", 7L)
        MvCatalog.ensureTables(session)
        CdfWatermarkCatalog.ensureTables(session)
        MvCatalog.lookup(session, entry.name) shouldBe Some(entry)
        CdfWatermarkCatalog.get(session, "db.mv_init", "db.source_init") shouldBe Some(7L)
        DeltaTable
          .forPath(session, s"$catalogPath/mv_metadata")
          .detail()
          .select("partitionColumns")
          .head()
          .getSeq[String](0) shouldBe Seq("name")
        DeltaTable
          .forPath(session, s"$catalogPath/cdf_watermarks")
          .detail()
          .select("partitionColumns")
          .head()
          .getSeq[String](0) shouldBe Seq("view_name")
      }
    }

    it("round-trips, indexes dependencies, advances, updates, and removes metadata") {
      val orders   = metadata("db.mv_orders", "db.orders")
      val customer = metadata("db.mv_customer", "db.customer")

      MvCatalog.ensureTables(spark)
      MvCatalog.upsert(spark, orders)
      MvCatalog.upsert(spark, customer)

      MvCatalog.lookup(spark, orders.name) shouldBe Some(orders)
      MvCatalog.list(spark).map(_.name).toSet shouldBe Set(orders.name, customer.name)
      MvCatalog.viewsForSource(spark, "db.orders").map(_.name) shouldBe Seq(orders.name)

      MvCatalog.advance(spark, orders.name, 9L)
      MvCatalog.updateProperties(spark, orders.name, Map("owner" -> "delta"))
      val updated = MvCatalog.lookup(spark, orders.name).get
      updated.lastVersion shouldBe 9L
      updated.properties shouldBe Map("owner" -> "delta")

      MvCatalog.remove(spark, orders.name)
      MvCatalog.lookup(spark, orders.name) shouldBe None
      MvCatalog.lookup(spark, customer.name) shouldBe Some(customer)
    }

    it("stores monotonic CDF watermarks in the shared Delta catalog") {
      CdfWatermarkCatalog.putAll(spark, "db.mv_orders", Map("db.orders" -> 4L, "db.lineitem" -> 7L))
      CdfWatermarkCatalog.put(spark, "db.mv_orders", "db.orders", 3L)

      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.orders") shouldBe Some(4L)
      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.lineitem") shouldBe Some(7L)

      CdfWatermarkCatalog.removeForBaseTable(spark, "db.lineitem")
      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.lineitem") shouldBe None
      CdfWatermarkCatalog.removeForView(spark, "db.mv_orders")
      CdfWatermarkCatalog.get(spark, "db.mv_orders", "db.orders") shouldBe None
    }

    it("atomically advances the MV version and retains monotonic CDF watermarks") {
      val entry = metadata("db.mv_atomic_cdf", "db.atomic_source")
      MvCatalog.upsert(spark, entry)
      MvCatalog.advanceWithCdfWatermarks(spark, entry.name, 5L, Map("db.atomic_source" -> 9L))
      MvCatalog.advanceWithCdfWatermarks(spark, entry.name, 4L, Map("db.atomic_source" -> 20L))
      MvCatalog.advanceWithCdfWatermarks(spark, entry.name, 6L, Map("db.atomic_source" -> 7L))

      val updated = MvCatalog.lookup(spark, entry.name).get
      updated.lastVersion shouldBe 6L
      updated.properties("owner") shouldBe "openivm"
      updated.changeWatermarks("db.atomic_source") shouldBe ChangeWatermark.DeltaVersion(9L)
    }

    it("reloads cached lookups and lists after grouped CDF metadata commits") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val catalogPath                                 = s"$root/cached-batch"
      val entries = (1 to 3).map(index => metadata(s"db.mv_cached_batch_$index", s"db.cached_source_$index"))

      withNewSession { reader =>
        reader.conf.set(FeatureGate.CatalogPathKey, catalogPath)
        entries.foreach(MvCatalog.upsert(reader, _))
        MvCatalog.list(reader).toSet shouldBe entries.toSet
        entries.foreach(entry => MvCatalog.lookup(reader, entry.name) shouldBe Some(entry))

        def catalogVersion: Long =
          DeltaTable.forPath(reader, s"$catalogPath/mv_metadata").history(1).select("version").head().getLong(0)

        for (version <- Seq(5L, 6L)) {
          val previous = catalogVersion
          val barrier  = new CyclicBarrier(entries.size)
          Await.result(
            Future.traverse(entries) { entry =>
              Future {
                withNewSession { writer =>
                  writer.conf.set(FeatureGate.CatalogPathKey, catalogPath)
                  val arrival = DeltaGroupCommit.refreshStarted()
                  try {
                    arrival.ready()
                    barrier.await()
                    DeltaMvCatalogBackend.advanceWithCdfWatermarks(
                      writer,
                      entry.name,
                      version,
                      Map(entry.sourceTables.head -> (version + 10L))
                    )
                  } finally arrival.close()
                }
              }
            },
            2.minutes
          )
          catalogVersion shouldBe previous + 1L
          val expected = entries.map(entry =>
            entry.copy(
              lastVersion = version,
              properties = entry.properties +
                (s"${MvMetadata.WatermarkKeyPrefix}${entry.sourceTables.head}" ->
                  ChangeWatermark.DeltaVersion(version + 10L).encode)
            )
          )
          if (version == 5L) MvCatalog.list(reader).toSet shouldBe expected.toSet
          expected.foreach(entry => MvCatalog.lookup(reader, entry.name) shouldBe Some(entry))
          MvCatalog.list(reader).toSet shouldBe expected.toSet
        }
      }
    }

    it("keeps a group commit open for a ready refresh that queues within one commit time") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val queue                                       = DeltaGroupCommit.forPath[Int](s"$root/group-straggler")
      val ready                                       = new CyclicBarrier(6)
      val batches                                     = mutable.ArrayBuffer.empty[Seq[Int]]
      def record(requests: Seq[Int]): Unit            = batches.synchronized { batches += requests.sorted; () }

      Await.result(
        Future.sequence((0 until 6).map { index =>
          Future(blocking {
            val arrival = DeltaGroupCommit.refreshStarted()
            try {
              arrival.ready()
              ready.await()
              if (index == 5) Thread.sleep(400L)
              queue.submit(index)(request => record(Seq(request)))(record)
            } finally arrival.close()
          })
        }),
        1.minute
      )

      batches shouldBe Seq(0 until 6)
    }

    it("bounds the wait for a ready refresh that never queues and ends it when that refresh closes") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val queue                                       = DeltaGroupCommit.forPath[Int](s"$root/group-absent")

      def leaderMillis(peerClosesAfterMs: Long): Long = {
        val peerReady  = new CountDownLatch(1)
        val leaderDone = new CountDownLatch(1)
        val peer = Future(blocking {
          val arrival = DeltaGroupCommit.refreshStarted()
          try {
            arrival.ready()
            peerReady.countDown()
            leaderDone.await(peerClosesAfterMs, TimeUnit.MILLISECONDS)
          } finally arrival.close()
        })
        val arrival = DeltaGroupCommit.refreshStarted()
        try {
          arrival.ready()
          peerReady.await()
          val start = System.nanoTime()
          queue.submit(0)(_ => ())(_ => fail("a lone request must commit on its own path"))
          (System.nanoTime() - start) / 1000000L
        } finally {
          arrival.close()
          leaderDone.countDown()
          Await.result(peer, 1.minute)
        }
      }

      leaderMillis(peerClosesAfterMs = 100L) should be < 800L
      leaderMillis(peerClosesAfterMs = 60000L) should be < 4000L
    }

    it("commits at once when no other ready refresh is expected") {
      val queue = DeltaGroupCommit.forPath[Int](s"$root/group-alone")
      val start = System.nanoTime()
      queue.submit(0)(_ => ())(_ => fail("a lone request must commit on its own path"))
      val arrival = DeltaGroupCommit.refreshStarted()
      try {
        arrival.ready()
        queue.submit(1)(_ => ())(_ => fail("a lone request must commit on its own path"))
      } finally arrival.close()

      (System.nanoTime() - start) / 1000000L should be < 500L
    }
    it("discards cached metadata when the catalog is recreated at the same Delta version") {
      val catalogPath = s"$root/cached-recreation"
      val tablePath   = s"$catalogPath/mv_metadata"
      val retained    = metadata("db.mv_cached_retained", "db.cached_original")
      val removed     = metadata("db.mv_cached_removed", "db.cached_removed")
      val replacement = metadata("db.mv_cached_retained", "db.cached_replacement")
      val added       = metadata("db.mv_cached_added", "db.cached_added")

      withNewSession { session =>
        session.conf.set(FeatureGate.CatalogPathKey, catalogPath)
        Seq(retained, removed).foreach(MvCatalog.upsert(session, _))
        MvCatalog.lookup(session, retained.name) shouldBe Some(retained)
        MvCatalog.list(session).toSet shouldBe Set(retained, removed)

        val originalId = DeltaTable.forPath(session, tablePath).detail().select("id").head().getString(0)
        val originalVersion =
          DeltaTable.forPath(session, tablePath).history(1).select("version").head().getLong(0)
        deleteDir(new File(tablePath))
        Seq(replacement, added).foreach(MvCatalog.upsert(session, _))

        DeltaTable.forPath(session, tablePath).detail().select("id").head().getString(0) should not be originalId
        DeltaTable.forPath(session, tablePath).history(1).select("version").head().getLong(0) shouldBe originalVersion
        MvCatalog.lookup(session, retained.name) shouldBe Some(replacement)
        MvCatalog.lookup(session, removed.name) shouldBe None
        MvCatalog.lookup(session, added.name) shouldBe Some(added)
        MvCatalog.list(session).toSet shouldBe Set(replacement, added)
      }
    }

    it("keeps shared watermark rows monotonic under synchronized concurrent puts") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val barrier                                     = new CyclicBarrier(2)

      val lower = Future {
        barrier.await()
        withNewSession(session => CdfWatermarkCatalog.put(session, "db.mv_orders_race", "db.orders_race", 3L))
      }
      val higher = Future {
        barrier.await()
        withNewSession(session => CdfWatermarkCatalog.put(session, "db.mv_orders_race", "db.orders_race", 9L))
      }

      Await.result(Future.sequence(Seq(lower, higher)), 2.minutes)

      CdfWatermarkCatalog.get(spark, "db.mv_orders_race", "db.orders_race") shouldBe Some(9L)
    }

    it("commits independent MV metadata upserts concurrently") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val barrier                                     = new CyclicBarrier(8)
      val entries = (1 to 8).map(index => metadata(s"db.mv_parallel_$index", s"db.source_$index"))

      Await.result(
        Future.traverse(entries)(entry =>
          Future {
            barrier.await()
            withNewSession(session => MvCatalog.upsert(session, entry))
          }
        ),
        2.minutes
      )

      entries.foreach(entry => MvCatalog.lookup(spark, entry.name) shouldBe Some(entry))
    }

    it("does not regress lastVersion while properties are updated concurrently") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val entry                                       = metadata("db.mv_property_race", "db.source_property_race")
      MvCatalog.upsert(spark, entry)

      val advances = Future {
        withNewSession(session => (2L to 20L).foreach(version => MvCatalog.advance(session, entry.name, version)))
      }
      val propertyUpdates = Future {
        withNewSession(session =>
          (1 to 20).foreach(index => MvCatalog.updateProperties(session, entry.name, Map("revision" -> index.toString)))
        )
      }
      // A full parallel Spark test run can saturate a small executor for well
      // over two minutes even though this race completes in under a minute in
      // isolation. Keep the assertion bounded without making it load-flaky.
      Await.result(Future.sequence(Seq(advances, propertyUpdates)), 5.minutes)

      val updated = MvCatalog.lookup(spark, entry.name).get
      updated.lastVersion shouldBe 20L
      updated.properties.keySet shouldBe Set("revision")
    }

    it("partitions each Delta catalog by its independent write key") {
      def partitionColumns(relativePath: String): Seq[String] =
        DeltaTable
          .forPath(spark, s"$root/catalog/$relativePath")
          .detail()
          .select("partitionColumns")
          .head()
          .getSeq[String](0)

      partitionColumns("mv_metadata") shouldBe Seq("name")
      partitionColumns("cdf_watermarks") shouldBe Seq("view_name")
    }
  }
}
