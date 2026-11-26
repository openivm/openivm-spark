package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.D3BusDrain
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier, Executors}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

final case class D3Scan(paths: Seq[String], files: Long, rows: Long)

final case class D3Cost(dataScans: Int, dataFiles: Long, dataRows: Long, logScans: Int, logFiles: Long, logRows: Long) {
  def +(other: D3Cost): D3Cost = D3Cost(
    dataScans + other.dataScans,
    dataFiles + other.dataFiles,
    dataRows + other.dataRows,
    logScans + other.logScans,
    logFiles + other.logFiles,
    logRows + other.logRows
  )
}

object D3ScanListener {
  val scans = new ConcurrentLinkedQueue[D3Scan]()

  def take(): Seq[D3Scan] = {
    val out  = Seq.newBuilder[D3Scan]
    var next = scans.poll()
    while (next != null) {
      out += next
      next = scans.poll()
    }
    out.result()
  }
}

/** Records every file scan of every query of every session, with its final file and row counts. */
class D3ScanListener extends QueryExecutionListener {
  private def leaves(plan: SparkPlan): Seq[FileSourceScanExec] = plan match {
    case scan: FileSourceScanExec        => Seq(scan)
    case adaptive: AdaptiveSparkPlanExec => leaves(adaptive.executedPlan)
    case stage: QueryStageExec           => leaves(stage.plan)
    case _: ReusedExchangeExec           => Seq.empty
    case other                           => other.children.flatMap(leaves)
  }

  private def record(qe: QueryExecution): Unit =
    Try(leaves(qe.executedPlan)).getOrElse(Seq.empty).foreach { scan =>
      def metric(name: String): Long = scan.metrics.get(name).map(_.value).getOrElse(0L)
      D3ScanListener.scans.add(
        D3Scan(scan.relation.location.rootPaths.map(_.toString), metric("numFiles"), metric("numOutputRows"))
      )
    }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = record(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = record(qe)
}

/** Bounds of shared CDF watermark reads against the per-view backend read on small, large, crossing, churning,
  * recreated and many roots. Runs unchanged on a build without sharing, where every read is the backend's.
  */
class D3WatermarkBoundSpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {

  private var spark: SparkSession = _
  private val root = {
    val dir = new File(s"target/test-d3-watermark-bound-${UUID.randomUUID().toString.take(8)}")
    require(dir.mkdirs() || dir.isDirectory)
    dir.getAbsolutePath
  }

  private val NoCost = D3Cost(0, 0L, 0L, 0, 0L, 0L)
  private val report = new java.util.LinkedHashMap[String, String]()

  private val schema = StructType(
    Seq(
      StructField("view_name", StringType, nullable = false),
      StructField("source", StringType, nullable = false),
      StructField("version", LongType, nullable = false)
    )
  )

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("openivm-spark-D3WatermarkBoundSpec")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.queryExecutionListeners", classOf[D3ScanListener].getName)
      .config("spark.sql.warehouse.dir", s"$root/warehouse")
      .config("spark.sql.shuffle.partitions", "4")
      .config(FeatureGate.CatalogBackendKey, "delta")
      .config(FeatureGate.CatalogPathKey, s"$root/catalog")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    rec("spark_version", spark.version)
    rec("sharing", Sharing.enabled)
  }

  override def afterAll(): Unit = {
    try {
      val dir = new File(sys.env.getOrElse("OPENIVM_D3_REPORT_DIR", "target"))
      require(dir.mkdirs() || dir.isDirectory)
      val variant = if (Sharing.enabled) "shared" else "v2"
      val body = report.synchronized {
        val out = new StringBuilder("{\n")
        val it  = report.entrySet().iterator()
        while (it.hasNext) {
          val entry = it.next()
          out.append("  ").append(json(entry.getKey)).append(": ").append(entry.getValue)
          if (it.hasNext) out.append(",")
          out.append("\n")
        }
        out.append("}\n").toString
      }
      Files.write(
        Paths.get(dir.getAbsolutePath, s"d3-watermark-bound-$variant-${spark.version}.json"),
        body.getBytes(StandardCharsets.UTF_8)
      )
      ()
    } finally {
      try if (spark != null) spark.stop()
      finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        deleteDir(new File(root))
      }
    }
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private def json(value: Any): String = value match {
    case s: String  => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case b: Boolean => b.toString
    case n: Int     => n.toString
    case n: Long    => n.toString
    case c: D3Cost =>
      s"""{"data_scans": ${c.dataScans}, "data_files": ${c.dataFiles}, "data_rows": ${c.dataRows}, """ +
        s""""log_scans": ${c.logScans}, "log_files": ${c.logFiles}, "log_rows": ${c.logRows}}"""
    case (a, b)     => Seq(a, b).map(json).mkString("[", ", ", "]")
    case xs: Seq[_] => xs.map(json).mkString("[", ", ", "]")
    case other      => json(String.valueOf(other))
  }

  private def rec(key: String, value: Any): Unit = report.synchronized { report.put(key, json(value)); () }

  /** Reflective view of the sharing state; every probe is empty on a build without it. */
  private object Sharing {
    private def module(name: String): Option[AnyRef] =
      Try(Class.forName(name).getField("MODULE$").get(null)).toOption

    private def field(owner: AnyRef, suffix: String): Option[AnyRef] =
      owner.getClass.getDeclaredFields.find(_.getName.endsWith(suffix)).map { f =>
        f.setAccessible(true)
        f.get(owner)
      }

    private def lru(owner: Option[AnyRef], suffix: String): Option[java.util.Map[AnyRef, AnyRef]] =
      owner.flatMap(field(_, suffix)).flatMap(field(_, "entries")).collect { case m: java.util.Map[_, _] =>
        m.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      }

    private def watermarks = module("org.openivm.spark.common.CdfWatermarkCatalog$")
    private def refreshes  = module("org.openivm.spark.common.DeltaLogRefreshes$")

    private def snapshot(map: Option[java.util.Map[AnyRef, AnyRef]]): Seq[(String, AnyRef)] =
      map.fold(Seq.empty[(String, AnyRef)]) { m =>
        m.synchronized {
          val out = Seq.newBuilder[(String, AnyRef)]
          val it  = m.entrySet().iterator()
          while (it.hasNext) {
            val entry = it.next()
            out += String.valueOf(entry.getKey) -> entry.getValue
          }
          out.result()
        }
      }

    private def rowCount(entry: AnyRef): Long =
      field(entry, "rows").collect { case a: Array[_] => a.length.toLong }.getOrElse(-1L)

    def enabled: Boolean = lru(watermarks, "committedVersions").isDefined && lru(watermarks, "largeRoots").isDefined

    def retained(tablePath: String): (Int, Long) = {
      val mine = snapshot(lru(watermarks, "committedVersions")).filter(_._1.contains(s"$tablePath/_delta_log"))
      mine.size -> mine.map(e => rowCount(e._2)).sum
    }

    def retainedTotal: (Int, Long) = {
      val all = snapshot(lru(watermarks, "committedVersions"))
      all.size -> all.map(e => rowCount(e._2)).sum
    }

    def flagged(tablePath: String): Boolean =
      snapshot(lru(watermarks, "largeRoots")).exists(_._1.contains(s"$tablePath/_delta_log"))

    def largeRoots: Int     = snapshot(lru(watermarks, "largeRoots")).size
    def gates: Int          = snapshot(lru(refreshes, "gates")).size
    def refreshRecords: Int = snapshot(lru(refreshes, "refreshes")).size
  }

  private def session(catalogPath: String): SparkSession = {
    val created = spark.newSession()
    created.conf.set(FeatureGate.CatalogPathKey, catalogPath)
    created
  }

  private def within[A](session: SparkSession)(f: => A): A = {
    SparkSession.setActiveSession(session)
    try f
    finally SparkSession.clearActiveSession()
  }

  private def table(catalogPath: String): String = s"$catalogPath/cdf_watermarks"

  private def viewName(prefix: String, i: Int): String = f"db.${prefix}_$i%03d"

  private def rows(prefix: String, views: Int, sources: Int, base: Long): Seq[(String, String, Long)] =
    for (v <- 0 until views; s <- 0 until sources) yield (viewName(prefix, v), s"db.s$s", base + v * 10L + s)

  /** Writes rows in one commit, creating the table with the backend's schema and partitioning when it is absent. */
  private def bulk(catalogPath: String, data: Seq[(String, String, Long)]): Unit = within(spark) {
    spark
      .createDataFrame(spark.sparkContext.parallelize(data.map { case (v, s, n) => Row(v, s, n) }, 1), schema)
      .write
      .format("delta")
      .mode("append")
      .partitionBy("view_name")
      .save(table(catalogPath))
  }

  private def expected(data: Seq[(String, String, Long)]): Map[(String, String), Long] =
    data.map { case (v, s, n) => (v, s) -> n }.toMap

  private def wanted(truth: Map[(String, String), Long], view: String, sources: Seq[String]): Map[String, Long] =
    sources.flatMap(s => truth.get(view -> s).map(s -> _)).toMap

  private def get(s: SparkSession, view: String, sources: String*): Map[String, Long] =
    within(s)(CdfWatermarkCatalog.getAll(s, view, sources))

  private def backendGet(s: SparkSession, view: String, sources: String*): Map[String, Long] =
    within(s)(CdfWatermarkBackend.forSession(s).getAll(s, view, sources))

  private def put(s: SparkSession, view: String, versions: (String, Long)*): Unit =
    within(s)(CdfWatermarkCatalog.putAll(s, view, versions.toMap))

  private def identity(tablePath: String): (String, Long) = within(spark) {
    val t = DeltaTable.forPath(spark, tablePath)
    t.detail().select("id").head().getString(0) -> t.history(1).select("version").head().getLong(0)
  }

  private def cost(scans: Seq[D3Scan], tablePath: String): D3Cost = {
    val mine        = scans.filter(_.paths.exists(_.contains(tablePath)))
    val (log, data) = mine.partition(_.paths.exists(_.contains(s"$tablePath/_delta_log")))
    D3Cost(data.size, data.map(_.files).sum, data.map(_.rows).sum, log.size, log.map(_.files).sum, log.map(_.rows).sum)
  }

  private def measured[A](tablePath: String)(f: => A): (A, D3Cost) = {
    D3BusDrain(spark.sparkContext)
    D3ScanListener.take()
    val result = f
    D3BusDrain(spark.sparkContext)
    result -> cost(D3ScanListener.take(), tablePath)
  }

  private val Sources = Seq("db.s0", "db.s1", "db.s9")

  describe("D3 CDF watermark sharing bounds") {
    it("reads a six-row root exactly, and when sharing retains only its rows and rescans nothing unchanged") {
      val catalogPath = s"$root/small"
      val tablePath   = table(catalogPath)
      val data        = rows("small", 3, 2, 10L)
      val truth       = expected(data)
      val views       = (0 until 3).map(viewName("small", _))
      bulk(catalogPath, data)
      val reader = session(catalogPath)

      def readAll(read: (SparkSession, String) => Map[String, Long]): Unit =
        views.foreach(v => read(reader, v) shouldBe wanted(truth, v, Sources))

      val (_, first)   = measured(tablePath)(readAll((s, v) => get(s, v, Sources: _*)))
      val (_, second)  = measured(tablePath)(readAll((s, v) => get(s, v, Sources: _*)))
      val (_, missing) = measured(tablePath)(get(reader, "db.small_missing", Sources: _*) shouldBe Map.empty)
      val (_, backend) = measured(tablePath)(readAll((s, v) => backendGet(s, v, Sources: _*)))

      backend.dataScans shouldBe views.size
      if (Sharing.enabled) {
        Sharing.retained(tablePath) shouldBe (1 -> data.size.toLong)
        Sharing.flagged(tablePath) shouldBe false
        first.dataScans shouldBe 1
        first.dataRows should (be >= data.size.toLong and be <= 65L)
        second.dataScans shouldBe 0
        missing.dataScans shouldBe 0
      } else {
        first.dataScans shouldBe views.size
        Sharing.retained(tablePath) shouldBe (0 -> 0L)
      }
      rec("small.first", first)
      rec("small.second", second)
      rec("small.missing", missing)
      rec("small.backend", backend)
      rec("small.retained", Sharing.retained(tablePath))
    }

    it("serves a root over 64 rows through the backend's per-view read, exactly, after one bounded probe") {
      val catalogPath = s"$root/large"
      val tablePath   = table(catalogPath)
      val data        = rows("large", 200, 2, 1000L)
      val truth       = expected(data)
      val sample      = Seq(0, 1, 63, 64, 137, 199).map(viewName("large", _))
      bulk(catalogPath, data)
      val reader = session(catalogPath)

      def readSample(read: (SparkSession, String) => Map[String, Long]): Unit =
        sample.foreach(v => read(reader, v) shouldBe wanted(truth, v, Sources))

      val (_, probe) =
        measured(tablePath)(get(reader, sample.head, Sources: _*) shouldBe wanted(truth, sample.head, Sources))
      val (_, steady)  = measured(tablePath)(readSample((s, v) => get(s, v, Sources: _*)))
      val (_, backend) = measured(tablePath)(readSample((s, v) => backendGet(s, v, Sources: _*)))
      get(reader, "db.large_missing", Sources: _*) shouldBe Map.empty
      get(reader, sample(2), "db.s1", "db.s7") shouldBe wanted(truth, sample(2), Seq("db.s1"))

      backend.dataScans shouldBe sample.size
      steady.dataScans shouldBe backend.dataScans
      steady.dataFiles shouldBe backend.dataFiles
      steady.dataRows shouldBe backend.dataRows
      Sharing.retained(tablePath) shouldBe (0 -> 0L)
      if (Sharing.enabled) Sharing.flagged(tablePath) shouldBe true
      rec("large.rows", data.size)
      rec("large.probe", probe)
      rec("large.steady", steady)
      rec("large.backend", backend)
      rec("large.flagged", Sharing.flagged(tablePath))
    }

    it("stays exact and monotone for concurrent readers while a root grows across the 64-row gate") {
      val catalogPath = s"$root/crossing"
      val tablePath   = table(catalogPath)
      val tracked     = viewName("cross", 0)
      val data        = rows("cross", 30, 2, 0L).map { case (v, s, n) => if (v == tracked && s == "db.s0") (v, s, 100L) else (v, s, n) }
      bulk(catalogPath, data)
      val finalStep = 6L
      val published = new AtomicLong(0L)
      val readers   = 6
      val barrier   = new CyclicBarrier(readers + 1)
      val deadline  = System.nanoTime() + 4.minutes.toNanos
      val pool      = Executors.newFixedThreadPool(readers + 1)
      implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        val writer = Future {
          val w = session(catalogPath)
          barrier.await()
          for (k <- 1L to finalStep) {
            put(w, viewName("crossnew", k.toInt), "db.s0" -> k, "db.s1" -> k)
            put(w, tracked, "db.s0" -> (100L + k))
            published.set(k)
          }
        }
        val reads = (1 to readers).map { _ =>
          Future {
            val r = session(catalogPath)
            barrier.await()
            var last       = 0L
            var iterations = 0
            while ((iterations < 4 || published.get() < finalStep) && System.nanoTime() < deadline) {
              val floor = 100L + published.get()
              val seen  = get(r, tracked, "db.s0")("db.s0")
              seen should be >= floor
              seen should be <= 100L + finalStep
              seen should be >= last
              last = seen
              iterations += 1
            }
            iterations
          }
        }
        Await.result(writer, 5.minutes)
        val iterations = Await.result(Future.sequence(reads), 5.minutes)
        published.get() shouldBe finalStep
        rec("crossing.reader_iterations", iterations)
      } finally {
        pool.shutdownNow()
        ()
      }

      val reader = session(catalogPath)
      val truth = expected(data) ++ Map((tracked, "db.s0") -> (100L + finalStep)) ++
        (1L to finalStep).flatMap(k => Seq("db.s0", "db.s1").map(s => (viewName("crossnew", k.toInt), s) -> k))
      truth.size shouldBe 72
      truth.keys.map(_._1).toSeq.distinct.foreach(v => get(reader, v, Sources: _*) shouldBe wanted(truth, v, Sources))
      Sharing.retained(tablePath) shouldBe (0 -> 0L)
      if (Sharing.enabled) Sharing.flagged(tablePath) shouldBe true
      rec("crossing.flagged", Sharing.flagged(tablePath))
    }

    it("keeps shared rows and log refresh records bounded across 72 roots, and an evicted root reads fresh") {
      val roots = (0 until 72).map(i => f"$root/many/r$i%02d")
      val truth = roots.indices.map(i => i -> rows(f"many$i%02d", 1, 3, i * 100L)).toMap
      roots.indices.foreach(i => bulk(roots(i), truth(i)))
      val reader       = spark.newSession()
      var maxRoots     = 0
      var maxRows      = 0L
      var maxGates     = 0
      var maxRefreshes = 0
      for (_ <- 1 to 2; i <- roots.indices) {
        reader.conf.set(FeatureGate.CatalogPathKey, roots(i))
        val view = viewName(f"many$i%02d", 0)
        get(reader, view, Sources: _*) shouldBe wanted(expected(truth(i)), view, Sources)
        val (retainedRoots, retainedRows) = Sharing.retainedTotal
        maxRoots = math.max(maxRoots, retainedRoots)
        maxRows = math.max(maxRows, retainedRows)
        maxGates = math.max(maxGates, Sharing.gates)
        maxRefreshes = math.max(maxRefreshes, Sharing.refreshRecords)
      }
      if (Sharing.enabled) {
        maxRoots should be <= 32
        maxRows should be <= 32L * 64L
        maxGates should be <= 64
        maxRefreshes should be <= 64
        Sharing.retained(table(roots.head)) shouldBe (0 -> 0L)
      } else {
        maxRoots shouldBe 0
        maxRows shouldBe 0L
      }

      val evicted = viewName("many00", 0)
      put(session(roots.head), evicted, "db.s0" -> 9999L)
      reader.conf.set(FeatureGate.CatalogPathKey, roots.head)
      get(reader, evicted, Sources: _*) shouldBe Map("db.s0" -> 9999L, "db.s1" -> 1L)
      rec("many.roots", roots.size)
      rec("many.max_retained_roots", maxRoots)
      rec("many.max_retained_rows", maxRows)
      rec("many.max_gates", maxGates)
      rec("many.max_refresh_records", maxRefreshes)
      rec("many.large_roots", Sharing.largeRoots)
    }

    it("rescans at most 65 rows per committed version of a small root under version churn") {
      val catalogPath = s"$root/churn-small"
      val tablePath   = table(catalogPath)
      val data        = rows("churns", 5, 2, 0L)
      bulk(catalogPath, data)
      val writer  = session(catalogPath)
      val reader  = session(catalogPath)
      var truth   = expected(data)
      var facade  = NoCost
      var backend = NoCost
      val perGet  = Seq.newBuilder[Long]
      for (c <- 1 to 12) {
        val view = viewName("churns", c % 5)
        put(writer, view, "db.s0" -> (1000L + c))
        truth = truth.updated(view -> "db.s0", 1000L + c)
        val (got, f) = measured(tablePath)(get(reader, view, Sources: _*))
        val (raw, b) = measured(tablePath)(backendGet(reader, view, Sources: _*))
        got shouldBe wanted(truth, view, Sources)
        raw shouldBe got
        b.dataScans shouldBe 1
        if (Sharing.enabled) f.dataRows should be <= 65L
        facade = facade + f
        backend = backend + b
        perGet += f.dataRows
      }
      rec("churn_small.facade", facade)
      rec("churn_small.backend", backend)
      rec("churn_small.facade_rows_per_get", perGet.result())
    }

    it("matches the backend's scan on every read of a large root under version churn") {
      val catalogPath = s"$root/churn-large"
      val tablePath   = table(catalogPath)
      val data        = rows("churnl", 150, 2, 0L)
      bulk(catalogPath, data)
      val writer = session(catalogPath)
      val reader = session(catalogPath)
      var truth  = expected(data)
      val first  = viewName("churnl", 0)
      val (_, probe) = measured(tablePath)(get(reader, first, Sources: _*) shouldBe wanted(truth, first, Sources))
      var facade     = NoCost
      var backend    = NoCost
      for (c <- 1 to 8) {
        val view = viewName("churnl", c * 17)
        put(writer, view, "db.s1" -> (5000L + c))
        truth = truth.updated(view -> "db.s1", 5000L + c)
        val (got, f) = measured(tablePath)(get(reader, view, Sources: _*))
        val (raw, b) = measured(tablePath)(backendGet(reader, view, Sources: _*))
        got shouldBe wanted(truth, view, Sources)
        raw shouldBe got
        f.dataScans shouldBe b.dataScans
        f.dataFiles shouldBe b.dataFiles
        f.dataRows shouldBe b.dataRows
        facade = facade + f
        backend = backend + b
      }
      Sharing.retained(tablePath) shouldBe (0 -> 0L)
      rec("churn_large.probe", probe)
      rec("churn_large.facade", facade)
      rec("churn_large.backend", backend)
    }

    for (freshLog <- Seq(false, true)) {
      it(s"reads exactly across a same-path small, large, small recreation at one version (fresh DeltaLog = $freshLog)") {
        val catalogPath = s"$root/recreate-$freshLog"
        val tablePath   = table(catalogPath)
        val reader      = session(catalogPath)
        val steps       = Seq(rows("rc", 3, 2, 10L), rows("rc", 50, 2, 50000L), rows("rc", 2, 2, 90000L))
        val probes      = (0 until 50).map(viewName("rc", _)) :+ "db.rc_missing"
        val identities = steps.zipWithIndex.map { case (data, step) =>
          if (step > 0) {
            deleteDir(new File(tablePath))
            if (freshLog) DeltaLog.clearCache()
          }
          bulk(catalogPath, data)
          val truth = expected(data)
          probes.foreach(v => get(reader, v, Sources: _*) shouldBe wanted(truth, v, Sources))
          identity(tablePath)
        }
        identities.map(_._1).distinct.size shouldBe steps.size
        identities.map(_._2).distinct shouldBe Seq(identities.head._2)
        Sharing.retained(tablePath)._2 should be <= 64L
        rec(s"recreate_$freshLog.flagged", Sharing.flagged(tablePath))
        rec(s"recreate_$freshLog.retained", Sharing.retained(tablePath))
      }
    }
  }
}
