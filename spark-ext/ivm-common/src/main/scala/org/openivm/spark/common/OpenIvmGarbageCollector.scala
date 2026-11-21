package org.openivm.spark.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.rocksdb.OpenIvmStateBarrier
import org.openivm.spark.insights.OpenIvmInsightJson
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, BoundedTelemetryRuntime, OpenIvmExecutionSpan}
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal

/**
 * Reclaims OpenIVM-owned warehouse storage that no durable state references.
 *
 * Staging, view-delta and trigger locations hold change rows referenced by staging records.
 * Pruning a fully consumed record tombstones its location in the same RocksDB write. The location
 * is deleted only once the state without the record is durable, so a driver restored from the
 * newest published state snapshot never reads a deleted location: callers [[prepare]] a plan
 * before publishing state and [[collect]] it after the publish returns. Locations no record ever
 * referenced, left by a driver that stopped between writing and recording them, are swept in the
 * background once older than [[FeatureGate.GcOrphanGracePeriodKey]], together with mirrored
 * telemetry spans older than [[FeatureGate.TelemetryOneLakeRetentionKey]]. The sweep works from
 * the plan's snapshot of references and touches only the file system.
 */
object OpenIvmGarbageCollector {
  private val log = LoggerFactory.getLogger(getClass)

  /** Garbage candidates read from the local state before it is published.
    *
    * @param tombstoned locations whose records were pruned
    * @param referenced locations referenced by staging records, MV metadata, or the cached view
    *                   deltas staging records reference
    */
  final case class Plan(tombstoned: Set[String], referenced: Set[String])

  private final case class GcTelemetryContext(operationId: Option[String]) {
    def contextStatus: String = if (operationId.isDefined) "present" else "missing"
  }

  private final case class GcTelemetrySpan(
      context: GcTelemetryContext,
      phase: String,
      startMonotonicNs: Long,
      startUtc: String
  )

  private final case class CleanupSummary(status: String, tombstonesDeleted: Long, sweepScheduled: Long)

  /** Staging-record roots and the depth of their record locations below each root. */
  private val OwnedRoots: Seq[(String, Int)] =
    Seq("_ivm/staging" -> 3, "_ivm/view_deltas" -> 2, "_openivm/triggers" -> 2)

  private final case class Sweep(
      hadoopConf: Configuration,
      warehouse: Path,
      stateRoot: String,
      protectedPaths: Set[String],
      cutoffMs: Long,
      telemetry: Option[(Path, Long)],
      gcTelemetry: Option[GcTelemetryContext],
      tombstoneCount: Long,
      referenceCount: Long
  )

  private val telemetryEmissionFailures                = new AtomicLong()
  private val defaultTelemetryOffer: String => Unit    = BoundedTelemetryRuntime.offer _
  @volatile private var telemetryOffer: String => Unit = defaultTelemetryOffer

  def telemetryEmissionFailureCount: Long = telemetryEmissionFailures.get()

  private[openivm] def setTelemetryOfferForTesting(offer: String => Unit): Unit = telemetryOffer = offer

  private[openivm] def resetTelemetryOfferForTesting(): Unit = telemetryOffer = defaultTelemetryOffer

  private val lastSweepMs                                         = new AtomicLong(0L)
  private[openivm] def setSweepClockForTesting(value: Long): Unit = lastSweepMs.set(value)

  private val executor: ExecutorService = Executors.newSingleThreadExecutor { runnable =>
    val thread = new Thread(runnable, "openivm-gc")
    thread.setDaemon(true)
    thread
  }

  /** Read a full plan for explicit collection, even when no work is due. */
  def prepare(spark: SparkSession): Option[Plan] = prepareCandidates(spark, sweepDue = true)

  /** A refresh may omit the entire plan, but must never hand collect incomplete references.
    * If a sweep becomes due after an idle preflight, a later refresh will plan it instead.
    */
  def prepareForRefresh(spark: SparkSession): Option[Plan] =
    prepareCandidates(spark, isSweepDue(spark, System.currentTimeMillis(), lastSweepMs.get()))

  private[openivm] def prepareForRefresh(spark: SparkSession, nowMs: Long): Option[Plan] =
    prepareCandidates(spark, isSweepDue(spark, nowMs, lastSweepMs.get()))

  private def isSweepDue(spark: SparkSession, nowMs: Long, lastMs: Long): Boolean =
    nowMs - lastMs >= FeatureGate.gcOrphanSweepIntervalMs(spark)

  private def prepareCandidates(spark: SparkSession, sweepDue: => Boolean): Option[Plan] =
    if (!FeatureGate.gcEnabled(spark)) None
    else {
      val telemetry = startTelemetry(spark, "plan")
      var status    = "success"
      var planned   = Option.empty[Plan]
      try {
        planned = StagingCatalog.garbageCollectionPaths(spark, sweepDue).map { stagingPaths =>
          val metadataReferences = MvCatalog
            .list(spark)
            .iterator
            .flatMap(_.properties.values)
            .filter(value => value.contains("/_ivm/") || value.contains("/_openivm/"))
            .toSet
          val recorded = stagingPaths.referenced
          Plan(
            stagingPaths.tombstoned,
            recorded ++ metadataReferences ++ StagingDeltaView.CachedViewDeltaRef.inputsOf(recorded)
          )
        }
        if (planned.isEmpty) status = "empty"
        planned
      } catch {
        case NonFatal(error) =>
          status = "failed"
          log.warn(s"openivm gc: cannot read garbage candidates: $error")
          None
      } finally {
        emitTelemetry(
          telemetry,
          status,
          Seq(
            "plan_count"      -> planned.fold(0L)(_ => 1L),
            "tombstone_count" -> planned.fold(0L)(_.tombstoned.size.toLong),
            "reference_count" -> planned.fold(0L)(_.referenced.size.toLong)
          )
        )
      }
    }

  /** Delete the tombstoned locations of `plan` that nothing references, then start a background
    * orphan sweep when one is due. Call only after the state `plan` was read from is durable.
    *
    * @param spark active session
    * @param plan  candidates from [[prepare]]
    */
  def collect(spark: SparkSession, plan: Option[Plan]): Unit =
    plan.foreach { candidates =>
      val telemetry = startTelemetry(spark, "cleanup")
      var summary   = CleanupSummary("success", 0L, 0L)
      try {
        OpenIvmStateBarrier.withMutation(spark) {
          summary = collectUnderBarrier(spark, candidates, telemetry.map(_.context))
        }
      } catch {
        case NonFatal(error) =>
          summary = summary.copy(status = "failed")
          throw error
      } finally {
        emitTelemetry(
          telemetry,
          summary.status,
          Seq(
            "tombstone_count"          -> candidates.tombstoned.size.toLong,
            "reference_count"          -> candidates.referenced.size.toLong,
            "tombstones_deleted_count" -> summary.tombstonesDeleted,
            "orphan_sweep_scheduled"   -> summary.sweepScheduled
          )
        )
      }
    }

  private def collectUnderBarrier(
      spark: SparkSession,
      candidates: Plan,
      gcTelemetry: Option[GcTelemetryContext]
  ): CleanupSummary = {
    var status            = "success"
    var tombstonesDeleted = 0L
    var sweepScheduled    = 0L
    try {
      val removed = candidates.tombstoned.filterNot(candidates.referenced.contains).filter { location =>
        try deleteLocation(spark.sessionState.newHadoopConf(), location)
        catch {
          case NonFatal(error) =>
            log.warn(s"openivm gc: cannot delete $location: $error")
            false
        }
      }
      tombstonesDeleted = removed.size.toLong
      StagingCatalog.forgetGarbage(spark, removed)
      if (removed.nonEmpty) log.info(s"openivm gc: deleted ${removed.size} consumed staging location(s)")
      val now  = System.currentTimeMillis()
      val last = lastSweepMs.get()
      if (isSweepDue(spark, now, last) && lastSweepMs.compareAndSet(last, now)) {
        val sweep = Sweep(
          spark.sessionState.newHadoopConf(),
          new Path(spark.conf.get("spark.sql.warehouse.dir")),
          OpenIvmStateBarrier.root(spark).toString,
          candidates.referenced ++ candidates.tombstoned,
          now - FeatureGate.gcOrphanGracePeriodMs(spark),
          FeatureGate
            .oneLakeTelemetryDir(spark)
            .map(dir => new Path(dir) -> (now - FeatureGate.telemetryOneLakeRetentionMs(spark))),
          gcTelemetry,
          candidates.tombstoned.size.toLong,
          candidates.referenced.size.toLong
        )
        executor.execute(() => runSweep(sweep))
        sweepScheduled = 1L
      }
    } catch {
      case NonFatal(error) =>
        status = "failed"
        log.warn(s"openivm gc: collection failed: $error")
    }
    CleanupSummary(status, tombstonesDeleted, sweepScheduled)
  }

  /** Run an orphan sweep synchronously, as [[collect]] does in the background.
    *
    * @param spark active session
    * @param plan  candidates from [[prepare]], taken after the orphans were abandoned
    * @param nowMs time the grace period and telemetry retention are measured from
    */
  private[openivm] def sweepNow(spark: SparkSession, plan: Plan, nowMs: Long): Unit = {
    val sweep = Sweep(
      spark.sessionState.newHadoopConf(),
      new Path(spark.conf.get("spark.sql.warehouse.dir")),
      OpenIvmStateBarrier.root(spark).toString,
      plan.referenced ++ plan.tombstoned,
      nowMs - FeatureGate.gcOrphanGracePeriodMs(spark),
      FeatureGate
        .oneLakeTelemetryDir(spark)
        .map(dir => new Path(dir) -> (nowMs - FeatureGate.telemetryOneLakeRetentionMs(spark))),
      captureTelemetryContext(spark),
      plan.tombstoned.size.toLong,
      plan.referenced.size.toLong
    )
    runSweep(sweep)
  }

  @volatile private[common] var afterSweepDeleteHookForTesting: Path => Unit = (_: Path) => ()

  private[common] def setAfterSweepDeleteHookForTesting(hook: Path => Unit): Unit =
    afterSweepDeleteHookForTesting = hook

  private def deleteSweepPath(sweep: Sweep, fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    val deleted = OpenIvmStateBarrier.withDbMutation(sweep.stateRoot) { fs.delete(path, recursive) }
    if (deleted) afterSweepDeleteHookForTesting(path)
    deleted
  }

  private def runSweep(sweep: Sweep): Unit = {
    val telemetry        = startTelemetry(sweep.gcTelemetry, "orphan_sweep")
    var status           = "success"
    var orphanCandidates = 0L
    var orphanDeleted    = 0L
    var telemetryDeleted = 0L
    try {
      val fs             = sweep.warehouse.getFileSystem(sweep.hadoopConf)
      val protectedPaths = sweep.protectedPaths.map(path => qualify(fs, path))
      OwnedRoots.foreach { case (relative, depth) =>
        locations(fs, new Path(sweep.warehouse, relative), depth).foreach { location =>
          orphanCandidates += 1L
          val path = fs.makeQualified(location.getPath)
          if (
            !protectedPaths.contains(path.toString) &&
            lastModified(fs, location) < sweep.cutoffMs &&
            deleteSweepPath(sweep, fs, path, recursive = true)
          ) orphanDeleted += 1L
        }
      }
      if (orphanDeleted > 0) log.info(s"openivm gc: deleted $orphanDeleted unreferenced staging location(s)")
      sweep.telemetry.foreach { case (root, expiryMs) =>
        val rootFs = root.getFileSystem(sweep.hadoopConf)
        if (rootFs.exists(root)) {
          rootFs.listStatus(root).foreach { span =>
            if (
              span.isFile && span.getModificationTime < expiryMs &&
              deleteSweepPath(sweep, rootFs, span.getPath, recursive = false)
            ) telemetryDeleted += 1L
          }
          if (telemetryDeleted > 0)
            log.info(s"openivm gc: deleted $telemetryDeleted expired telemetry span(s) under $root")
        }
      }
    } catch {
      case NonFatal(error) =>
        status = "failed"
        log.warn(s"openivm gc: orphan sweep failed: $error")
    } finally {
      emitTelemetry(
        telemetry,
        status,
        Seq(
          "tombstone_count"         -> sweep.tombstoneCount,
          "reference_count"         -> sweep.referenceCount,
          "orphan_candidates_count" -> orphanCandidates,
          "orphan_deleted_count"    -> orphanDeleted,
          "telemetry_deleted_count" -> telemetryDeleted
        )
      )
    }
  }

  private def startTelemetry(spark: SparkSession, phase: String): Option[GcTelemetrySpan] =
    startTelemetry(captureTelemetryContext(spark), phase)

  private def startTelemetry(context: Option[GcTelemetryContext], phase: String): Option[GcTelemetrySpan] =
    context.map(value => GcTelemetrySpan(value, phase, System.nanoTime(), Instant.now().toString))

  private def captureTelemetryContext(spark: SparkSession): Option[GcTelemetryContext] = {
    val enabled =
      try spark.sparkContext.getConf.getBoolean(BoundedTelemetryQueue.EnabledKey, defaultValue = false)
      catch {
        case NonFatal(_) =>
          recordTelemetryFailure()
          false
      }
    if (!enabled) None
    else {
      val operationId =
        try {
          Option(spark.sparkContext.getLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty)).filter(_.nonEmpty)
        } catch {
          case NonFatal(_) =>
            recordTelemetryFailure()
            None
        }
      Some(GcTelemetryContext(operationId))
    }
  }

  private def emitTelemetry(span: Option[GcTelemetrySpan], status: String, counts: Seq[(String, Long)]): Unit =
    span.foreach { value =>
      try {
        val fields = Seq[(String, Any)](
          "event_type"            -> "openivm_gc",
          "clock_domain_id"       -> MonotonicClockDomain.id,
          "scope"                 -> "garbage_collection",
          "phase"                 -> value.phase,
          "operation_id"          -> value.context.operationId.orNull,
          "context_status"        -> value.context.contextStatus,
          "status"                -> status,
          "start_utc"             -> value.startUtc,
          "start_monotonic_ns"    -> value.startMonotonicNs,
          "inclusive_duration_ns" -> (System.nanoTime() - value.startMonotonicNs)
        ) ++ counts
        telemetryOffer(OpenIvmInsightJson.obj(fields: _*))
      } catch {
        case NonFatal(_) => recordTelemetryFailure()
      }
    }

  private def recordTelemetryFailure(): Unit = {
    val count = telemetryEmissionFailures.incrementAndGet()
    if (count == 1L) log.warn("openivm gc: telemetry emission failed (failure_count=1)")
  }

  private def deleteLocation(hadoopConf: Configuration, location: String): Boolean = {
    val path = new Path(location)
    val fs   = path.getFileSystem(hadoopConf)
    !fs.exists(path) || fs.delete(path, true) || !fs.exists(path)
  }

  private def qualify(fs: FileSystem, location: String): String =
    try fs.makeQualified(new Path(location)).toString
    catch { case NonFatal(_) => location }

  private def locations(fs: FileSystem, root: Path, depth: Int): Seq[FileStatus] =
    if (!fs.exists(root)) Seq.empty
    else
      (1 until depth).foldLeft(fs.listStatus(root).toSeq.filter(_.isDirectory)) { (level, _) =>
        level.flatMap(parent => fs.listStatus(parent.getPath).toSeq.filter(_.isDirectory))
      }

  /** Newest modification time of a location and its direct children, which include its Delta log. */
  private def lastModified(fs: FileSystem, location: FileStatus): Long =
    (location.getModificationTime +: fs.listStatus(location.getPath).toSeq.map(_.getModificationTime)).max
}
