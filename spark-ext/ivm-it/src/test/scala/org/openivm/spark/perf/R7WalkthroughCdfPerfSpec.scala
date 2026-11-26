package org.openivm.spark.perf

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{CdfWatermarkCatalog, FabricCommandBridge, FeatureGate, MvCatalog, QueryLogExport}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, BoundedTelemetryRuntime}
import org.openivm.spark.testkit.TestPools

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

private[perf] object R7WalkthroughCdfPerfSpec {
  val Schema     = "openivm.r7-walkthrough-perf/v1"
  val WorkloadId = "primary:cdf-walkthrough-six-mv-insert"
  val Database   = "openivm_demo"
  val SeedRows   = "(1,'AAA',10),(2,'BBB',20),(3,'CCC',30)"
  val DeltaRows  = "(101,'AAA',7),(102,'BBB',11),(103,'CCC',13)"
  val CapturePins: Map[String, String] = Map(
    "live_workload_capture_sha256"    -> "b1d22ce297f33f63a04cb1fe505e8365eb1e524e1089db9acd38e58a8c5549b2",
    "live_workload_definition_sha256" -> "e31177a9a13aba537f167d9bec6cb3ccbd81f0a0561fc96c58476bf50a9dafdb",
    "capture_helper_sha256"           -> "db89d3990b13054090bda5a63837ea23f6340c9f9897a08f5779c60d540f0ae0"
  )
  val PollMsEnv    = "OPENIVM_R7_INSIGHTS_POLL_MS"
  val ExtraConfEnv = "OPENIVM_R7_TELEMETRY_CONF"
  val ExtraPrefix  = "spark.openivm.telemetry."
  val Second: Long = 1000000000L

  final case class Request(
      view: String,
      requestId: String,
      submitNs: Long,
      bridgeStartNs: Long,
      bridgeEndNs: Long,
      pollEndNs: Long,
      drainEndNs: Long,
      completedEndNs: Long,
      queryLogEndNs: Long,
      endNs: Long,
      polls: Int,
      insightEvents: Int,
      captureStatus: String,
      drained: Boolean,
      span: Option[JsonNode],
      queryLog: Option[JsonNode],
      healthBefore: Map[String, Long],
      healthAfter: Map[String, Long],
      errors: Seq[String]
  )

  final case class Batch(title: String, workers: Int, startNs: Long, requests: Seq[Request]) {
    def endNs: Long       = requests.map(_.endNs).max
    def returnEndNs: Long = requests.map(_.bridgeEndNs).max
  }

  /** Telemetry settings both arms must pin identically, e.g. `spark.openivm.telemetry.taskMetrics.enabled=true`. */
  def extraTelemetryConf: Map[String, String] =
    sys.env
      .get(ExtraConfEnv)
      .map(
        _.split(",").toSeq
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { pair =>
            val idx = pair.indexOf('=')
            require(idx > 0, s"$ExtraConfEnv entries must be key=value: $pair")
            val key = pair.substring(0, idx).trim
            require(isTelemetryPin(key), s"$ExtraConfEnv only admits non-path, non-queue $ExtraPrefix* keys: $key")
            key -> pair.substring(idx + 1).trim
          }
          .toMap
      )
      .getOrElse(Map.empty)

  /** Observer settings that select instrumentation; the queue and destination paths are fixed by the harness. */
  def isTelemetryPin(key: String): Boolean =
    key.startsWith(ExtraPrefix) && !key.startsWith("spark.openivm.telemetry.queue.") &&
      !key.startsWith("spark.openivm.telemetry.oneLakeDir") &&
      key != FeatureGate.TelemetryUriKey && key != FeatureGate.TelemetryCampaignIdKey &&
      key != FeatureGate.TelemetryCorrelationIdKey && key != FeatureGate.TelemetryPhaseKey

  def telemetryPins(all: Map[String, String]): Map[String, String] = all.filter { case (k, _) => isTelemetryPin(k) }
}

/** Primary R7 workload: the six-MV CDF parallel REFRESH from live walkthrough capture `b1d22ce2`.
  *
  * Mirrors `concurrent_demo` in helper `db89d399`: two CDF Delta sources seeded with three rows, six grouped COUNT/SUM
  * views, an identical three-row INSERT, a one-worker REFRESH over `serial_events`, DROP, re-CREATE over
  * `parallel_events`, the same INSERT, then the scored six-worker REFRESH. Every command is a full live `execute`
  * through [[FabricCommandBridge]]: INSIGHTS polling to a terminal status, `awaitDrain(10000)`, exactly one
  * identity-matched completed span, query-log finalization and release. The scored clock runs from the batch start
  * before submission to the last request's durable completion; command return is recorded separately.
  *
  * Catalog and state-sync paths carry modeled latency; telemetry, source, MV and receipt I/O is real local disk. The
  * result is local relative evidence, never an OneLake or Fabric measurement.
  */
class R7WalkthroughCdfPerfSpec extends IvmParitySpecBase("r7-walkthrough-cdf") with CdfMode {
  import R7WalkthroughCdfPerfSpec._
  import StateGuardReport.{ms, union}

  private val remote    = new File(warehouseDir, "state-sync-remote")
  private val queue     = new File(warehouseDir, "telemetry-queue")
  private val telemetry = new File(warehouseDir, "telemetry")
  private val receipts  = new File(warehouseDir, "receipts")
  private val tables    = new File(warehouseDir, "tables")
  private val sync      = new File(remote, ".openivm-state-sync")
  private val mapper    = new ObjectMapper
  private val label     = sys.env.getOrElse(StateGuardReport.RunLabelEnv, "r7-walkthrough")
  private val campaign  = label
  private val runId     = s"${label}_walkthrough"
  private val pollMs    = sys.env.get(PollMsEnv).map(_.toLong).getOrElse(1000L)
  private val views     = (1 to 6).map(i => s"concurrent_$i")

  Seq(remote, queue, telemetry, receipts, tables).foreach(_.mkdirs())

  override protected def sparkMaster: String = "local[8]"

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone"                   -> "UTC",
    "spark.sql.shuffle.partitions"                 -> "4",
    "spark.plugins"                                -> "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin",
    FeatureGate.CatalogBackendKey                  -> "delta",
    FeatureGate.CatalogPathKey                     -> new File(warehouseDir, "_openivm_catalog").toURI.toString,
    FeatureGate.StatePathKey                       -> new File(warehouseDir, "local-state").getAbsolutePath,
    FeatureGate.StateSyncUriKey                    -> remote.toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey           -> "false",
    FeatureGate.ProfileRefreshKey                  -> "true",
    FeatureGate.QueryLogEnabledKey                 -> "true",
    FeatureGate.WindowRunningIncrementalEnabledKey -> "true",
    FeatureGate.TelemetryUriKey                    -> telemetry.toURI.toString,
    BoundedTelemetryQueue.EnabledKey               -> "true",
    BoundedTelemetryQueue.LocalUriKey              -> queue.toURI.toString,
    "fs.file.impl"                                 -> classOf[StateGuardLocalFileSystem].getName,
    "fs.file.impl.disable.cache"                   -> "true",
    "fs.AbstractFileSystem.file.impl"              -> classOf[StateGuardLocalAbstractFs].getName
  ) ++ extraTelemetryConf

  override def afterAll(): Unit =
    try {
      StateGuardFsInjection.disable()
      StateGuardFsInjection.reset()
    } finally super.afterAll()

  private def name(short: String): String = s"$Database.$short"

  private def ident(view: String): TableIdentifier = TableIdentifier(view, Some(Database))

  private def query(source: String, i: Int): String =
    s"SELECT symbol, COUNT(*) AS events, SUM(amount * $i) AS total FROM ${name(source)} GROUP BY symbol"

  private def phaseOf(text: String): String = {
    val upper = text.trim.toUpperCase
    if (upper.startsWith("CREATE MATERIALIZED VIEW")) "create"
    else if (upper.startsWith("REFRESH MATERIALIZED VIEW")) "refresh"
    else "data_change"
  }

  private def health: Map[String, Long] =
    BoundedTelemetryRuntime.health
      .map { h =>
        Map(
          "accepted"          -> h.accepted,
          "published"         -> h.published,
          "dropped_full"      -> h.droppedFull,
          "dropped_oversized" -> h.droppedOversized,
          "dropped_closed"    -> h.droppedClosed,
          "dropped_contended" -> h.droppedContended,
          "export_failures"   -> h.exportFailures,
          "manifest_failures" -> h.manifestFailures,
          "worker_failures"   -> h.workerFailures,
          "queued"            -> h.queued.toLong
        )
      }
      .getOrElse(Map.empty)

  private def delta(before: Map[String, Long], after: Map[String, Long]): Map[String, Long] =
    after.map { case (k, v) => k -> (v - before.getOrElse(k, 0L)) }

  private def deltaVersion(table: String): Long =
    spark.sql(s"DESCRIBE HISTORY $table LIMIT 1").collect().head.getAs[Long]("version")

  private def cdfProperty(table: String): String =
    spark
      .sql(s"SHOW TBLPROPERTIES $table")
      .collect()
      .collectFirst { case r if r.getString(0) == "delta.enableChangeDataFeed" => r.getString(1) }
      .getOrElse("")

  private def completedSpans(requestId: String, target: String): (Seq[JsonNode], Seq[String]) = {
    val dir  = new File(new File(telemetry, "completed"), "v1")
    val all  = Option(dir.listFiles()).toSeq.flatten.filter(_.getName.endsWith(".json")).map(f => mapper.readTree(f))
    val mine = all.filter(_.path("request_id").asText("") == requestId)
    val problems = mine.flatMap { v =>
      val identity =
        v.path("schema_id").asText("") == "openivm.execution-span" && v.path("schema_version").asInt(-1) == 1 &&
          v.path("campaign_id").asText("") == campaign && v.path("correlation_id").asText("") == requestId &&
          v.path("dbt_node_id").asText("") == target
      val duration = v.path("duration_ms")
      Seq(
        if (identity) None else Some(s"completed span identity/schema mismatch for $requestId"),
        if (duration.isIntegralNumber && duration.asLong() >= 0L) None
        else Some(s"invalid completed library duration for $requestId")
      ).flatten
    }
    (mine, problems)
  }

  private def pollFinished(done: Boolean, status: String, hasMore: Boolean, afterWorkerNs: Long): Boolean =
    if (!done || hasMore) false
    else if (status == "complete" || status == "failed") true
    else afterWorkerNs >= 30L * Second

  private def describe(t: Throwable): String = s"${t.getClass.getName}: ${String.valueOf(t.getMessage).take(400)}"

  /** The live helper's `execute`, without display and row previews. */
  private def execute(title: String, text: String, target: String, capture: Boolean, submitNs: Long): Request = {
    val requestId    = "req_" + UUID.randomUUID().toString.replace("-", "")
    val jobGroup     = s"${runId}_$requestId"
    val healthBefore = health
    val finished     = new CountDownLatch(1)
    val begun        = new CountDownLatch(1)
    val errors       = new ConcurrentLinkedQueue[String]()
    val bridgeStart  = new AtomicLong(0L)
    val bridgeEnd    = new AtomicLong(0L)
    val worker = new Thread(() =>
      try {
        bridgeStart.set(System.nanoTime())
        begun.countDown()
        FabricCommandBridge.executeCommand(
          spark,
          requestId,
          runId,
          campaign,
          target,
          if (capture) "materialized_view" else null,
          if (capture) target else null,
          phaseOf(text),
          title,
          text,
          jobGroup,
          capture
        )
        ()
      } catch {
        case t: Throwable =>
          errors.add(s"command failed: ${describe(t)}")
          ()
      } finally {
        bridgeEnd.set(System.nanoTime())
        finished.countDown()
      }
    )
    worker.setDaemon(true)
    worker.start()

    var cursor        = 0L
    var status        = "missing"
    var polls         = 0
    var insightEvents = 0
    var workerDoneNs  = -1L
    var polling       = true
    try {
      while (polling) {
        require(System.nanoTime() - submitNs < 1200L * Second, "command exceeded its 20-minute bound")
        val done = finished.getCount == 0
        if (begun.getCount == 0) {
          val page = spark.sql(s"SHOW OPENIVM INSIGHTS FOR REQUEST '$requestId' AFTER $cursor LIMIT 1000").collect()
          require(page.nonEmpty && page.head.getAs[String]("record_type") == "status", "invalid insight page")
          status = page.head.getAs[String]("capture_status")
          cursor = page.head.getAs[Long]("next_sequence")
          insightEvents += page.length - 1
          polls += 1
          if (done && workerDoneNs < 0L) workerDoneNs = System.nanoTime()
          val after = if (workerDoneNs < 0L) -1L else System.nanoTime() - workerDoneNs
          if (pollFinished(done, status, page.head.getAs[Boolean]("has_more"), after)) polling = false
        } else if (done) polling = false
        if (polling) {
          if (finished.getCount == 0) Thread.sleep(pollMs) else finished.await(pollMs, TimeUnit.MILLISECONDS)
        }
      }
    } catch {
      case t: Throwable =>
        errors.add(s"observer failed: ${describe(t)}")
        spark.sparkContext.cancelJobGroup(jobGroup)
    } finally worker.join(15000L)
    if (worker.isAlive) errors.add(s"native operation $requestId has not terminated")
    val pollEnd = System.nanoTime()

    val drained     = BoundedTelemetryRuntime.awaitDrain(10000L)
    val drainEnd    = System.nanoTime()
    val healthAfter = health
    val span = if (capture) {
      val (mine, problems) = completedSpans(requestId, target)
      problems.foreach(errors.add)
      if (mine.size != 1) errors.add(s"expected exactly one completed span for $requestId; found ${mine.size}")
      mine.headOption
    } else None
    val completedEnd = System.nanoTime()

    var log: Option[JsonNode] = None
    if (capture) {
      val deadline = System.nanoTime() + 30L * Second
      var waiting  = true
      while (waiting && System.nanoTime() < deadline) {
        val snapshot = mapper.readTree(QueryLogExport.snapshotJson(spark, requestId, 2000, 4 * 1024 * 1024))
        log = Some(snapshot)
        val st = snapshot.path("status").asText("")
        if ((st == "complete" || st == "failed") && snapshot.path("pending_flushes").asInt(-1) == 0) waiting = false
        else Thread.sleep(500L)
      }
      val complete = log.exists { l =>
        val failure = l.path("failure")
        l.path("capture_complete").asBoolean(false) && (failure.isMissingNode || failure.isNull)
      }
      if (!complete) errors.add(s"query-log capture incomplete or failed for $requestId")
    }
    val queryLogEnd = System.nanoTime()

    if (status != "complete") errors.add(s"INSIGHTS terminal status for $requestId is $status")
    if (!drained) errors.add(s"telemetry drain snapshot timed out for $requestId")
    span.filter(_.path("operation").asText("") == "refresh").foreach { s =>
      if (
        s.path("effective_refresh_type").asText("") == "FULL_REFRESH" ||
        s.path("outcome").asText("") == "full_refresh_executed"
      ) errors.add(s"library executed FULL_REFRESH for incremental request $requestId")
    }
    val receipt = new File(receipts, requestId)
    receipt.mkdirs()
    val body = new java.util.LinkedHashMap[String, AnyRef]()
    body.put("request_id", requestId)
    body.put("title", title)
    body.put("capture_status", status)
    body.put("errors", errors.asScala.toList.asJava)
    body.put("span", span.orNull)
    body.put("query_log", log.orNull)
    Files.write(new File(receipt, "report.json").toPath, mapper.writeValueAsBytes(body))
    if (status == "complete" || status == "failed")
      spark.sql(s"OPENIVM INSIGHTS RELEASE REQUEST '$requestId'").collect()
    if (log.exists(l => l.path("capture_complete").asBoolean(false) && l.path("pending_flushes").asInt(-1) == 0))
      QueryLogExport.release(spark, requestId)
    Request(
      target,
      requestId,
      submitNs,
      bridgeStart.get(),
      bridgeEnd.get(),
      pollEnd,
      drainEnd,
      completedEnd,
      queryLogEnd,
      System.nanoTime(),
      polls,
      insightEvents,
      status,
      drained,
      span,
      log,
      healthBefore,
      healthAfter,
      errors.asScala.toList
    )
  }

  /** `concurrent_demo.batch`: the clock starts before submission and ends when every request is durable. */
  private def batch(title: String, commands: Seq[(String, String)], workers: Int): Batch = {
    val start = new AtomicLong(0L)
    val requests = TestPools.withPool(workers) { implicit ec =>
      start.set(System.nanoTime())
      val futures = commands.map { case (view, text) =>
        Future(execute(s"$title: $view", text, view, capture = true, System.nanoTime()))
      }
      Await.result(Future.sequence(futures), 20.minutes)
    }
    Batch(title, workers, start.get(), requests)
  }

  private def verify(source: String): Seq[String] =
    views.zipWithIndex.flatMap { case (view, idx) =>
      val registered = MvCatalog.lookup(spark, ident(view)).map(_.refreshTypeName)
      val incremental =
        if (registered.exists(t => !t.toUpperCase.contains("FULL"))) None
        else Some(s"$view is not registered as incremental: $registered")
      val parity = scala.util
        .Try(assertMvCorrect(name(view), query(source, idx + 1)))
        .failed
        .toOption
        .map(e => s"$view parity: ${String.valueOf(e.getMessage).take(200)}")
      incremental.toSeq ++ parity.toSeq
    }

  private def generation: Option[String] =
    Option(new File(sync, "commits").listFiles()).toSeq.flatten
      .filter(_.getName.endsWith(".commit"))
      .map(_.getName.stripSuffix(".commit"))
      .sorted
      .lastOption

  private def committedSnapshot(gen: String): Boolean = {
    val lines   = Files.readAllLines(new File(sync, s"commits/$gen.commit").toPath, StandardCharsets.UTF_8)
    val archive = new File(sync, s"snapshots/$gen/snapshot.zip")
    lines.size >= 3 && lines.get(0) == "OPENIVM_STATE_SNAPSHOT_V2" && lines.get(1) == gen && archive.isFile &&
    archive.length() == lines.get(2).trim.toLong
  }

  private def queueEvents(ids: Set[String]): Vector[JsonNode] =
    Option(queue.listFiles()).toVector.flatten.filter(_.getName.endsWith(".jsonl")).sortBy(_.getName).flatMap { f =>
      Files
        .readAllLines(f.toPath, StandardCharsets.UTF_8)
        .asScala
        .iterator
        .map(line => mapper.readTree(line))
        .filter(n => ids.contains(n.path("request_id").asText("")))
        .toVector
    }

  private def interval(n: JsonNode): StateGuardInterval = {
    val start = n.path("start_monotonic_ns").asLong()
    StateGuardInterval(start, start + n.path("inclusive_ns").asLong())
  }

  // Absent phase events are unavailable, never a measured zero.
  private def sumMs(xs: Seq[StateGuardInterval]): Option[Double] =
    if (xs.isEmpty) None else Some(ms(xs.map(_.lengthNs).sum))

  private def unionMs(xs: Seq[StateGuardInterval]): Option[Double] =
    if (xs.isEmpty) None else Some(ms(union(xs).map(_.lengthNs).getOrElse(0L)))

  private def requestRow(b: Batch, r: Request, events: Vector[JsonNode]): Map[String, Any] = {
    def at(ns: Long): Double = ms(ns - b.startNs)
    val phases =
      events.filter(n => n.path("request_id").asText("") == r.requestId && n.path("event_type").asText("") == "phase")
    def named(p: String => Boolean): Seq[StateGuardInterval] =
      phases.filter(n => p(n.path("phase").asText(""))).map(interval)
    Map[String, Any](
      "view"                     -> r.view,
      "request_id"               -> r.requestId,
      "submit_at_ms"             -> at(r.submitNs),
      "bridge_start_at_ms"       -> at(r.bridgeStartNs),
      "return_at_ms"             -> at(r.bridgeEndNs),
      "insights_terminal_at_ms"  -> at(r.pollEndNs),
      "drain_return_at_ms"       -> at(r.drainEndNs),
      "completed_checked_at_ms"  -> at(r.completedEndNs),
      "query_log_final_at_ms"    -> at(r.queryLogEndNs),
      "durable_at_ms"            -> at(r.endNs),
      "command_return_ms"        -> ms(r.bridgeEndNs - r.bridgeStartNs),
      "request_durable_ms"       -> ms(r.endNs - r.submitNs),
      "insights_after_return_ms" -> ms(r.pollEndNs - r.bridgeEndNs),
      "drain_ms"                 -> ms(r.drainEndNs - r.pollEndNs),
      "query_log_finalize_ms"    -> ms(r.queryLogEndNs - r.completedEndNs),
      "insight_polls"            -> r.polls,
      "insight_events"           -> r.insightEvents,
      "capture_status"           -> r.captureStatus,
      "drain_snapshot_completed" -> r.drained,
      "library_duration_ms"      -> r.span.map(_.path("duration_ms").asLong()),
      "operation"                -> r.span.map(_.path("operation").asText("")),
      "outcome"                  -> r.span.map(_.path("outcome").asText("")),
      "effective_refresh_type"   -> r.span.map(_.path("effective_refresh_type").asText("")),
      "pending_delta_count"      -> r.span.map(_.path("pending_delta_count").asLong(-1L)),
      "source_versions"          -> r.span.map(s => mapper.writeValueAsString(s.path("source_versions"))),
      "query_log_records"        -> r.queryLog.map(_.path("records").size()),
      "state_barrier_wait_ms"    -> sumMs(named(_ == "state_barrier_wait")),
      "state_barrier_hold_ms"    -> sumMs(named(_ == "state_barrier_hold")),
      "catalog_finalize_ms"      -> unionMs(named(_.startsWith("finalize_"))),
      "engine_exec_ms"           -> unionMs(named(_.startsWith("execute_refresh_sql"))),
      "phase_events"             -> phases.size,
      "clock_domains"            -> phases.map(_.path("clock_domain_id").asText("")).distinct.sorted,
      "queue_delta"              -> delta(r.healthBefore, r.healthAfter),
      "errors"                   -> r.errors
    )
  }

  private def batchReport(b: Batch, events: Vector[JsonNode]): Map[String, Any] =
    Map[String, Any](
      "title"                   -> b.title,
      "workers"                 -> b.workers,
      "request_ids"             -> b.requests.map(_.requestId),
      "durable_ms"              -> ms(b.endNs - b.startNs),
      "return_ms"               -> ms(b.returnEndNs - b.startNs),
      "return_to_durable_ms"    -> ms(b.endNs - b.returnEndNs),
      "max_concurrent_commands" -> concurrency(b),
      "requests"                -> b.requests.map(r => requestRow(b, r, events))
    )

  private def concurrency(b: Batch): Int =
    StateGuardReport.maxConcurrent(b.requests.map(r => StateGuardInterval(r.bridgeStartNs, r.bridgeEndNs)))

  it("measures the live six-MV CDF parallel REFRESH through the walkthrough durable boundary", StateGuardPerfTag) {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $Database").collect()
    Seq("serial_events", "parallel_events").foreach { short =>
      spark
        .sql(
          s"CREATE TABLE ${name(short)} USING DELTA LOCATION '${new File(tables, short).toURI}' " +
            "TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS SELECT CAST(id AS INT) AS event_id, symbol, " +
            s"CAST(amount AS INT) AS amount FROM VALUES $SeedRows AS v(id,symbol,amount)"
        )
        .collect()
    }
    val creates = ArrayBuffer.empty[Batch]
    def create(source: String, title: String): Seq[String] = {
      creates += batch(
        title,
        views.zipWithIndex.map { case (v, i) =>
          name(v) -> s"CREATE MATERIALIZED VIEW ${name(v)} AS ${query(source, i + 1)}"
        },
        6
      )
      verify(source)
    }
    def insert(title: String, source: String): Request =
      execute(title, s"INSERT INTO ${name(source)} VALUES $DeltaRows", name(source), capture = false, System.nanoTime())
    val latency = StateGuardLatency.modeled(StateGuardReport.latencyScale)
    def refresh(
        title: String,
        workers: Int
    ): (Batch, Map[String, Long], Map[String, Long], Option[String], Option[String]) = {
      val generationBefore = generation
      val healthBefore     = health
      StateGuardFsInjection.reset()
      StateGuardFsInjection.configure(latency)
      val b =
        try batch(title, views.map(v => name(v) -> s"REFRESH MATERIALIZED VIEW ${name(v)}"), workers)
        finally StateGuardFsInjection.disable()
      (b, StateGuardFsInjection.snapshot(), delta(healthBefore, health), generationBefore, generation)
    }

    val serialCreate                      = create("serial_events", "Concurrent CREATE — six MVs")
    val serialInsert                      = insert("Equivalent serial-arm insert", "serial_events")
    val (serial, serialFs, serialQ, _, _) = refresh("Serial REFRESH — six MVs", 1)
    val serialVerify                      = verify("serial_events")
    views.foreach(v => spark.sql(s"DROP MATERIALIZED VIEW ${name(v)}").collect())
    val parallelCreate = create("parallel_events", "Concurrent CREATE — identical twin baseline")

    val source         = name("parallel_events")
    val confMode       = spark.conf.getOption(FeatureGate.ChangeFeedModeKey).getOrElse("<unset>")
    val gateMode       = FeatureGate.changeFeedMode(spark).value
    val sourceCdf      = cdfProperty(source)
    val versionBefore  = deltaVersion(source)
    val parallelInsert = insert("Equivalent parallel-arm insert", "parallel_events")
    val versionAfter   = deltaVersion(source)
    val (primary, fsOps, primaryQ, generationBefore, generationAfter) = refresh("Parallel REFRESH — six MVs", 6)
    val primaryVerify                                                 = verify("parallel_events")

    val watermarks = views.map { v =>
      val meta      = MvCatalog.lookup(spark, ident(v))
      val qualified = meta.map(m => m.name.database.fold(m.name.table)(db => s"$db.${m.name.table}")).getOrElse(v)
      name(v) -> meta.toSeq
        .flatMap(_.sourceTables)
        .map(s => s -> CdfWatermarkCatalog.get(spark, qualified, s).getOrElse(-1L))
        .toMap
    }.toMap
    val watermarkProof = watermarks.values.forall(m => m.nonEmpty && m.values.forall(_ == versionAfter))
    val modeProof = confMode == "cdf" && gateMode == "cdf" && sourceCdf == "true" &&
      versionAfter == versionBefore + 1L && watermarkProof

    val ids       = primary.requests.map(_.requestId)
    val outcomes  = primary.requests.map(_.span.map(_.path("outcome").asText("")).getOrElse(""))
    val types     = primary.requests.map(_.span.map(_.path("effective_refresh_type").asText("")).getOrElse(""))
    val fullCount = outcomes.zip(types).count { case (o, t) => o == "full_refresh_executed" || t == "FULL_REFRESH" }
    val incrementalCount = outcomes.zip(types).count { case (o, t) =>
      o == "incremental_executed" && t.nonEmpty && t != "FULL_REFRESH"
    }
    val requestErrors =
      (creates.toSeq :+ serial :+ primary).flatMap(_.requests.flatMap(_.errors)) ++
        serialInsert.errors ++ parallelInsert.errors
    val drops = Seq("dropped_full", "dropped_oversized", "dropped_closed", "dropped_contended")
      .map(k => primaryQ.getOrElse(k, 0L))
      .sum
    val boundary = primary.requests.forall(r =>
      r.errors.isEmpty && r.drained && r.captureStatus == "complete" && r.span.nonEmpty && r.queryLog.nonEmpty
    )
    val modeledActive = StateGuardFsInjection.Areas.forall(area =>
      fsOps.iterator.collect { case (k, n) if k.startsWith(s"$area.") => n }.sum > 0L
    )
    val failures = requestErrors ++ serialCreate ++ serialVerify ++ parallelCreate ++ primaryVerify
    val gates = Map[String, Boolean](
      "parity"                 -> (serialCreate ++ serialVerify ++ parallelCreate ++ primaryVerify).isEmpty,
      "incremental_only"       -> (incrementalCount == 6 && fullCount == 0),
      "mode_proof"             -> modeProof,
      "durable_boundary"       -> boundary,
      "no_request_errors"      -> requestErrors.isEmpty,
      "queue_no_drops"         -> (drops == 0L && primaryQ.getOrElse("export_failures", 0L) == 0L),
      "six_concurrent"         -> (concurrency(primary) == 6),
      "unique_request_ids"     -> (ids.distinct.size == 6),
      "modeled_latency_active" -> modeledActive,
      "state_sync_committed"   -> generationAfter.exists(a => generationBefore.forall(_ < a) && committedSnapshot(a))
    )
    val events = queueEvents((serial.requests ++ primary.requests).map(_.requestId).toSet)
    val confKeys = Seq(
      FeatureGate.ChangeFeedModeKey,
      FeatureGate.CatalogBackendKey,
      FeatureGate.StateSyncAsyncEnabledKey,
      FeatureGate.ProfileRefreshKey,
      FeatureGate.QueryLogEnabledKey,
      FeatureGate.WindowRunningIncrementalEnabledKey,
      BoundedTelemetryQueue.EnabledKey,
      BoundedTelemetryQueue.CapacityKey,
      "spark.sql.shuffle.partitions",
      "spark.sql.session.timeZone",
      "spark.plugins"
    )
    val telemetryConf = telemetryPins(spark.conf.getAll)
    StateGuardReport.write(
      "r7-walkthrough-cdf.json",
      Map[String, Any](
        "schema"           -> Schema,
        "workload"         -> WorkloadId,
        "role"             -> "primary",
        "evidence_class"   -> StateGuardReport.EvidenceClass,
        "label"            -> label,
        "head_sha"         -> StateGuardReport.headSha,
        "spark_target"     -> StateGuardReport.sparkTarget,
        "spark_version"    -> spark.version,
        "master"           -> spark.sparkContext.master,
        "capture_pins"     -> CapturePins,
        "insights_poll_ms" -> pollMs,
        "conf"             -> confKeys.map(k => k -> spark.conf.getOption(k).getOrElse("<unset>")).toMap,
        "telemetry_conf"   -> telemetryConf,
        "latency_profile" -> Map(
          "name"             -> latency.name,
          "scale"            -> latency.scale,
          "catalog_read_ms"  -> latency.catalogReadMs,
          "catalog_write_ms" -> latency.catalogWriteMs,
          "state_read_ms"    -> latency.stateReadMs,
          "state_write_ms"   -> latency.stateWriteMs,
          "state_rename_ms"  -> latency.stateRenameMs
        ),
        "storage" -> ("file:// local Delta; modeled catalog/state-sync I/O only; telemetry, source, MV and receipt " +
          "I/O unmodeled; no OneLake or Fabric platform catalog"),
        "mode_proof" -> Map(
          "conf_mode"             -> confMode,
          "feature_gate_mode"     -> gateMode,
          "source"                -> source,
          "source_cdf_property"   -> sourceCdf,
          "source_version_before" -> versionBefore,
          "source_version_after"  -> versionAfter,
          "cdf_watermarks"        -> watermarks
        ),
        "primary" -> Map(
          "batch"                        -> primary.title,
          "workers"                      -> primary.workers,
          "request_ids"                  -> ids,
          "durable_ms"                   -> ms(primary.endNs - primary.startNs),
          "return_ms"                    -> ms(primary.returnEndNs - primary.startNs),
          "incremental_count"            -> incrementalCount,
          "full_refresh_count"           -> fullCount,
          "outcomes"                     -> outcomes,
          "effective_refresh_types"      -> types,
          "queue_delta"                  -> primaryQ,
          "fs_ops"                       -> fsOps,
          "state_sync_generation_before" -> generationBefore,
          "state_sync_generation_after"  -> generationAfter
        ),
        "batches" -> Map(
          "serial_create"    -> batchReport(creates.head, Vector.empty),
          "serial_refresh"   -> (batchReport(serial, events) ++ Map("fs_ops" -> serialFs, "queue_delta" -> serialQ)),
          "parallel_create"  -> batchReport(creates(1), Vector.empty),
          "parallel_refresh" -> batchReport(primary, events)
        ),
        "gates" -> gates,
        "coverage" -> Map(
          "primary_durable_ms"       -> "measured",
          "primary_return_ms"        -> "measured",
          "library_duration_ms"      -> "measured:completed-span",
          "phase_breakdown"          -> "measured:nested-non-additive",
          "onelake_io"               -> "unavailable",
          "telemetry_export_latency" -> "not-exercised:local-file",
          "serial_vs_parallel"       -> "measured:concurrency-comparison-not-code-gain"
        ),
        "failures" -> failures,
        "notes" -> Seq(
          "Local relative evidence: catalog and state-sync latency is modeled; this is not an OneLake measurement.",
          "Scored clock: batch start before submission through every request's live durable boundary.",
          "Return clock is the last FabricCommandBridge return; drain return is not physical persistence.",
          "Per-request phase spans are nested and non-additive; they are diagnostics, not a critical path."
        )
      )
    )
    StateGuardReport.writeLines("r7-walkthrough-events.jsonl", events.map(n => mapper.writeValueAsString(n)))

    withClue(failures.take(10).mkString("; ")) {
      gates.filterNot(_._2).keys.toSeq.sorted shouldBe empty
    }
  }
}
