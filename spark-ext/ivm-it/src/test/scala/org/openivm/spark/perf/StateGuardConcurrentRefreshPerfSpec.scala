package org.openivm.spark.perf

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.openivm.spark.common.{FeatureGate, RefreshSqlLogAsyncFlusher}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}
import org.openivm.spark.telemetry.OpenIvmExecutionSpan.{
  StateSyncEvidence,
  StateSyncPublicationEvidence,
  StateSyncRequestEvidence
}
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, OpenIvmExecutionSpan}
import org.openivm.spark.testkit.TestPools
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

private[perf] object StateGuardConcurrentRefreshPerfSpec {
  final case class ViewRow(
      report: Map[String, Any],
      engine: Option[StateGuardInterval],
      holds: Seq[StateGuardInterval]
  )

  final case class Run(
      requestIds: Seq[String],
      submitNs: Seq[Long],
      ackNs: Seq[Long],
      startNs: Long,
      drainStartNs: Long,
      drainEndNs: Long,
      evidence: Seq[StateSyncEvidence]
  )
}

/** Opt-in concurrent REFRESH measurement for the six-MV state-guard shape recorded on Fabric.
  *
  * Six aggregate MVs over one source are refreshed at the same instant against a `file://` state-sync remote and the
  * `delta` catalog backend. [[StateGuardFsInjection]] adds modeled per-operation latency to state-sync and catalog
  * paths. The numbers are local relative evidence for comparing old and new code, not OneLake measurements.
  */
class StateGuardConcurrentRefreshPerfSpec extends IvmParitySpecBase("state-guard-perf") with CdfMode with Eventually {
  import StateGuardConcurrentRefreshPerfSpec.{Run, ViewRow}
  import StateGuardReport.{ms, union}

  private val MvCount = 6

  private val workDir   = new File(s"target/state-guard-perf-${UUID.randomUUID().toString.take(8)}").getAbsoluteFile
  private val remoteDir = new File(workDir, "state-sync-remote")
  private val queueDir  = new File(workDir, "telemetry-queue")
  private val mapper    = new ObjectMapper()

  workDir.mkdirs()
  remoteDir.mkdirs()
  queueDir.mkdirs()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(Span(90, Seconds), Span(1, Seconds))

  override protected def sparkMaster: String = "local[8]"

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.plugins"                          -> "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin",
    BoundedTelemetryQueue.EnabledKey         -> "true",
    BoundedTelemetryQueue.LocalUriKey        -> queueDir.toURI.toString,
    BoundedTelemetryQueue.CapacityKey        -> "65536",
    FeatureGate.CatalogBackendKey            -> StateGuardReport.catalogBackend,
    FeatureGate.StateSyncUriKey              -> remoteDir.toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey     -> "false",
    FeatureGate.ProfileRefreshKey            -> "true",
    FeatureGate.QueryLogEnabledKey           -> "true",
    "fs.file.impl"                           -> classOf[StateGuardLocalFileSystem].getName,
    "fs.file.impl.disable.cache"             -> "true",
    "fs.AbstractFileSystem.file.impl"        -> classOf[StateGuardLocalAbstractFs].getName
  )

  override def afterAll(): Unit =
    try {
      StateGuardFsInjection.disable()
      StateGuardFsInjection.reset()
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
    } finally {
      try super.afterAll()
      finally deleteDir(workDir)
    }

  private def text(node: JsonNode, field: String): String = node.path(field).asText("")

  private def telemetry: Vector[JsonNode] =
    Option(queueDir.listFiles()).toVector.flatten.filter(_.getName.endsWith(".jsonl")).sortBy(_.getName).flatMap {
      file => Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.map(line => mapper.readTree(line))
    }

  private def phaseInterval(node: JsonNode): StateGuardInterval = {
    val start = node.path("start_monotonic_ns").asLong()
    StateGuardInterval(start, start + node.path("inclusive_ns").asLong())
  }

  private def syncRoot: File = new File(remoteDir, ".openivm-state-sync")

  private def commitCount: Int =
    Option(new File(syncRoot, "commits").listFiles()).map(_.count(_.getName.endsWith(".commit"))).getOrElse(0)

  private def snapshotCount: Int =
    Option(new File(syncRoot, "snapshots").listFiles()).map(_.count(_.isDirectory)).getOrElse(0)

  private def expectedSql(index: Int): String =
    s"SELECT symbol, COUNT(*) AS events, SUM(amount * $index) AS total FROM sgperf_events GROUP BY symbol"

  private def applyDelta(step: Int): Unit = {
    sql(
      s"INSERT INTO sgperf_events SELECT concat('sym_', cast(id % 20 as string)), " +
        s"cast(${1000 + step * 100} + id as int) FROM range(40)"
    )
    sql(s"DELETE FROM sgperf_events WHERE symbol = 'sym_${step % 20}' AND amount % 2 = 0")
  }

  private def runBatch(batchId: String): Run = {
    val ids      = (1 to MvCount).map(i => s"sgperf-$batchId-mv$i")
    val evidence = new ConcurrentLinkedQueue[StateSyncEvidence]()
    val gate     = new CountDownLatch(1)
    val submit   = new Array[Long](MvCount)
    val ack      = new Array[Long](MvCount)
    OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting { e => evidence.add(e); () }
    try {
      var start = 0L
      TestPools.withPool(MvCount) { implicit ec =>
        val futures = (0 until MvCount).map { idx =>
          Future {
            spark.sparkContext.setLocalProperty("openivm.request_id", ids(idx))
            gate.await()
            submit(idx) = System.nanoTime()
            try spark.sql(s"REFRESH MATERIALIZED VIEW sgperf_mv${idx + 1}").collect()
            finally {
              ack(idx) = System.nanoTime()
              spark.sparkContext.setLocalProperty("openivm.request_id", null)
            }
          }
        }
        start = System.nanoTime()
        gate.countDown()
        Await.result(Future.sequence(futures), 15.minutes)
      }
      val drainStart = System.nanoTime()
      RefreshSqlLogAsyncFlusher.awaitQuiescence(120000L) shouldBe true
      val drainEnd = System.nanoTime()
      Run(ids, submit.toSeq, ack.toSeq, start, drainStart, drainEnd, evidence.asScala.toSeq)
    } finally OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
  }

  it("measures six concurrent incremental refreshes against modeled state-sync and catalog latency", StateGuardPerfTag) {
    sql("CREATE TABLE sgperf_events(symbol STRING, amount INT) USING DELTA")
    sql("INSERT INTO sgperf_events SELECT concat('sym_', cast(id % 20 as string)), cast(id as int) FROM range(200)")
    (1 to MvCount).foreach { i =>
      sql(
        s"CREATE MATERIALIZED VIEW sgperf_mv$i AS SELECT symbol, COUNT(*) AS events, SUM(amount * $i) AS total " +
          "FROM sgperf_events GROUP BY symbol"
      )
    }

    applyDelta(0)
    runBatch("warmup")

    applyDelta(1)
    val profile = StateGuardLatency.modeled(StateGuardReport.latencyScale)
    StateGuardFsInjection.reset()
    StateGuardFsInjection.configure(profile)
    val run =
      try runBatch("measured")
      finally StateGuardFsInjection.disable()
    val fsOps = StateGuardFsInjection.snapshot()

    val requests     = run.evidence.flatMap(_.requests)
    val publications = run.evidence.flatMap(_.publications).groupBy(_.publicationId).map { case (_, v) => v.head }
    val publicationById: Map[String, StateSyncPublicationEvidence] = publications.map(p => p.publicationId -> p).toMap

    var events: Vector[JsonNode] = Vector.empty
    eventually {
      events = telemetry
      run.requestIds.foreach { id =>
        val mine = events.filter(n => text(n, "request_id") == id)
        mine.exists(n => text(n, "event_type") == "phase" && text(n, "phase") == "state_barrier_hold") shouldBe true
        mine.exists(n =>
          text(n, "event_type") != "phase" && text(n, "operation") == "refresh" && n.has("effective_refresh_type")
        ) shouldBe true
      }
    }

    val offset = (ns: Long) => ms(ns - run.startNs)

    val views = (0 until MvCount).map { idx =>
      val id      = run.requestIds(idx)
      val mine    = events.filter(n => text(n, "request_id") == id)
      val phases  = mine.filter(n => text(n, "event_type") == "phase")
      def named(p: String => Boolean): Seq[StateGuardInterval] =
        phases.filter(n => p(text(n, "phase"))).map(phaseInterval)
      val waits    = named(_ == "state_barrier_wait")
      val holds    = named(_ == "state_barrier_hold")
      val engine   = union(named(_.startsWith("execute_refresh_sql")))
      val finalize = union(named(_.startsWith("finalize_")))
      val advance  = named(_ == "finalize_catalog_advance")
      val span = mine
        .find(n => text(n, "event_type") != "phase" && text(n, "operation") == "refresh" && n.has("effective_refresh_type"))
        .getOrElse(fail(s"missing refresh span for $id"))
      val reqs: Seq[StateSyncRequestEvidence] = requests.filter(_.parentRequestId.contains(id))
      val pubs                                = reqs.map(_.publicationId).distinct.flatMap(publicationById.get)
      val capture = pubs.flatMap(_.phases.filter(_.operation == "spark.backup.barrier_wait")).map(_.durationNs).sum
      val publishing =
        union(pubs.map(p => StateGuardInterval(p.startMonotonicNs, p.startMonotonicNs + p.durationNs)))

      def at(interval: Option[StateGuardInterval], pick: StateGuardInterval => Long): Double =
        interval.map(i => offset(pick(i))).getOrElse(0.0)

      val holdSpan = union(holds)
      val row = Map[String, Any](
        "mv_index"                   -> (idx + 1),
        "name"                       -> s"sgperf_mv${idx + 1}",
        "wall_ms"                    -> ms(run.ackNs(idx) - run.submitNs(idx)),
        "refresh_type"               -> text(span, "effective_refresh_type"),
        "outcome"                    -> text(span, "outcome"),
        "submit_to_ack_ms"           -> ms(run.ackNs(idx) - run.submitNs(idx)),
        "state_barrier_wait_ms"      -> ms(waits.map(_.lengthNs).sum),
        "state_barrier_hold_ms"      -> ms(holds.map(_.lengthNs).sum),
        "capture_wait_ms"            -> ms(capture),
        "catalog_finalize_ms"        -> ms(finalize.map(_.lengthNs).getOrElse(0L)),
        "catalog_advance_ms"         -> ms(advance.map(_.lengthNs).sum),
        "publication_queue_wait_ms"  -> ms(reqs.map(_.queueWaitNs).sum),
        "publication_ms"             -> ms(pubs.map(_.durationNs).sum),
        "engine_exec_ms"             -> ms(engine.map(_.lengthNs).getOrElse(0L)),
        "submit_at_ms"               -> offset(run.submitNs(idx)),
        "ack_at_ms"                  -> offset(run.ackNs(idx)),
        "barrier_acquired_at_ms"     -> at(holdSpan, _.startNs),
        "barrier_released_at_ms"     -> at(holdSpan, _.endNs),
        "engine_start_at_ms"         -> at(engine, _.startNs),
        "engine_end_at_ms"           -> at(engine, _.endNs),
        "catalog_finalize_start_at_ms" -> at(finalize, _.startNs),
        "catalog_finalize_end_at_ms" -> at(finalize, _.endNs),
        "publication_start_at_ms"    -> at(publishing, _.startNs),
        "publication_end_at_ms"      -> at(publishing, _.endNs),
        "state_sync_requests"        -> reqs.size,
        "state_sync_acknowledged"    -> (reqs.nonEmpty && reqs.forall(r => r.status == "success") &&
          reqs.exists(_.acknowledgedSnapshotGenerationId.nonEmpty))
      )
      ViewRow(row, engine, holds)
    }

    val engineIntervals = views.flatMap(_.engine)
    val holdIntervals   = views.flatMap(_.holds)
    val publicVisible   = views.map(_.report)

    StateGuardFsInjection.disable()
    val parityFailures = (1 to MvCount).flatMap { i =>
      scala.util
        .Try(assertMvCorrect(s"sgperf_mv$i", expectedSql(i)))
        .failed
        .toOption
        .map(e => s"sgperf_mv$i: ${String.valueOf(e.getMessage).take(200)}")
    }
    val outcomes        = publicVisible.map(_("outcome").toString)
    val incrementalOnly = outcomes.forall(o => o.nonEmpty && !o.contains("full_refresh"))
    val acknowledged    = publicVisible.forall(_("state_sync_acknowledged") == true)
    val commits         = commitCount
    val snapshots       = snapshotCount
    val latestGenerations =
      requests.flatMap(_.acknowledgedSnapshotGenerationId).distinct
    val snapshotsPresent = latestGenerations.nonEmpty &&
      latestGenerations.forall(gen => new File(syncRoot, s"snapshots/$gen/snapshot.zip").isFile)
    val stateSyncConsistent = acknowledged && commits >= 1 && snapshotsPresent

    val makespanNs = run.drainEndNs - run.startNs
    val notes = Seq(
      "Local relative evidence: state-sync and catalog filesystem latency is modeled, not an OneLake measurement.",
      "Per-view spans are nested and non-additive; Delta I/O is not proven to be the cause of any wait."
    ) ++ parityFailures ++ Option(
      run.evidence.map(_.droppedRequests).sum
    ).filter(_ > 0L).map(n => s"$n state-sync request evidence records were dropped").toSeq

    val byArea = StateGuardFsInjection.Areas.map { area =>
      area -> StateGuardFsInjection.Ops.map(op => op -> fsOps(s"$area.$op")).toMap
    }.toMap

    val report = Map[String, Any](
      "schema"        -> "openivm.state-guard-perf/v1",
      "evidence_class" -> StateGuardReport.EvidenceClass,
      "spark_target"  -> StateGuardReport.sparkTarget,
      "head_sha"      -> StateGuardReport.headSha,
      "catalog_backend" -> StateGuardReport.catalogBackend,
      "label"         -> sys.env.getOrElse(StateGuardReport.RunLabelEnv, ""),
      "latency_profile" -> Map(
        "name"             -> profile.name,
        "catalog_read_ms"  -> profile.catalogReadMs,
        "catalog_write_ms" -> profile.catalogWriteMs,
        "state_read_ms"    -> profile.stateReadMs,
        "state_write_ms"   -> profile.stateWriteMs,
        "state_rename_ms"  -> profile.stateRenameMs,
        "scale"            -> profile.scale
      ),
      "end_to_end_ms"           -> ms(makespanNs),
      "makespan_ms"             -> ms(makespanNs),
      "drain_ms"                -> ms(run.drainEndNs - run.drainStartNs),
      "serial_sum_ms"           -> ms((0 until MvCount).map(i => run.ackNs(i) - run.submitNs(i)).sum),
      "max_concurrent_engine"   -> StateGuardReport.maxConcurrent(engineIntervals),
      "engine_overlap_ms"       -> ms(StateGuardReport.overlapNs(engineIntervals, 2)),
      "barrier_hold_overlap_ms" -> ms(StateGuardReport.overlapNs(holdIntervals, 2)),
      "views"                   -> publicVisible,
      "counts" -> Map(
        "incremental_count" -> outcomes.count(o => o.nonEmpty && !o.contains("full_refresh")),
        "full_refresh_count" -> outcomes.count(_.contains("full_refresh")),
        "fs_ops"            -> byArea,
        "state_sync" -> Map(
          "publications"   -> publications.count(_.published),
          "commits"        -> commits,
          "snapshots"      -> snapshots,
          "bytes_uploaded" -> publications.map(_.counts.get("uploaded_bytes").flatten.getOrElse(0L)).sum
        )
      ),
      "gates" -> Map(
        "parity"                -> parityFailures.isEmpty,
        "incremental_only"      -> incrementalOnly,
        "state_sync_consistent" -> stateSyncConsistent
      ),
      "notes" -> notes
    )
    StateGuardReport.write("state-guard-perf.json", report)
    StateGuardReport.writeLines(
      "state-guard-events.jsonl",
      events.filter(n => run.requestIds.contains(text(n, "request_id"))).map(n => mapper.writeValueAsString(n))
    )

    parityFailures shouldBe empty
    incrementalOnly shouldBe true
    stateSyncConsistent shouldBe true
  }
}
