package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.FeatureGate
import org.openivm.spark.telemetry.{BoundedTelemetryRuntime, OpenIvmExecutionSpan}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.NonFatal

/**
 * Mirrors the local RocksDB state tree (`<statePath>/_openivm`) to and from a
 * Hadoop-FS URI (OneLake `abfss://…` on Microsoft Fabric) so OpenIVM's IVM state
 * survives an ephemeral / recycled cloud Spark session.
 *
 * Gated entirely by [[FeatureGate.StateSyncUriKey]]: when the key is unset (every
 * local / HDFS deployment) all methods are no-ops and the on-disk behaviour is
 * unchanged. When set:
 *
 *   - [[maybeRestore]] runs once per Spark application, before any RocksDB opens,
 *     and downloads the remote tree if the local tree is absent/empty.
 *   - [[backupNow]] synchronously publishes a complete immutable generation.
 *     Concurrent requests share a pass only if they arrived before its request
 *     barrier. Failed or interrupted uploads never overwrite earlier snapshots.
 *   - [[backupAsync]] coalesces background requests through the same serialized
 *     generation publisher. Legacy flat-layout snapshots remain readable.
 */
private[rocksdb] final case class StateSyncPhaseStart(
    operation: String,
    workClass: String,
    startNanos: Long,
    startUtc: String
)

object OpenIvmStateSync {
  private val log = LoggerFactory.getLogger(getClass)

  private[rocksdb] final case class BackupStateSnapshot(running: Boolean, requested: Boolean)

  private final case class BackupRequest(
      requestId: String,
      parent: OpenIvmExecutionSpan.StateSyncParentContext,
      span: Option[OpenIvmExecutionSpan],
      requestedAtNanos: Long,
      requestedAtUtc: String,
      mode: String,
      captureTelemetry: Boolean,
      receiptWriter: Option[OpenIvmExecutionSpan.StateSyncReceiptWriter]
  )

  private final case class AsyncRequest(request: BackupRequest, afterPublish: () => Unit)
  private final case class SynchronousRequest(request: BackupRequest, completion: CompletableFuture[Unit])
  private final class GuardedBinding(val root: String, val store: GuardedSnapshotDecisions.ArchiveStore) {
    val backup                                              = new BackupState
    val restored                                            = new CompletableFuture[Unit]()
    val restoring                                           = new AtomicBoolean(false)
    var latest: Option[GuardedSnapshotDecisions.Generation] = None
  }

  private val guardedBindings = new java.util.WeakHashMap[SparkContext, GuardedBinding]()

  private[rocksdb] final case class StateSyncRequestReceipt(
      requestId: String,
      requestedMutationGeneration: Long,
      capturedMutationGeneration: Option[Long],
      publicationGeneration: Option[String],
      published: Boolean,
      reused: Boolean,
      coverageProven: Boolean,
      acknowledged: Boolean,
      outcome: String,
      archiveSha256: Option[String] = None,
      archiveBytes: Option[Long] = None
  )

  private final case class SnapshotPublication(
      generation: String,
      capturedMutationGeneration: Option[Long],
      archiveSha256: String,
      archiveBytes: Long
  )

  private final case class SynchronousCandidateRequest(
      requestId: String,
      request: BackupRequest,
      requestedMutationGeneration: Long,
      coverageEligible: Boolean,
      completion: CompletableFuture[CandidateCompletion],
      receiptRecorded: CompletableFuture[Unit]
  ) {
    // Accessed only under synchronousMonitor; delivery happens on the requesting thread.
    var admission = Option.empty[CandidateAdmission]
    var evidence = Vector.empty[OpenIvmExecutionSpan.StateSyncRequestEvidence]
    var publications = Vector.empty[OpenIvmExecutionSpan.StateSyncPublicationEvidence]
  }

  private final case class CandidateAdmission(publicationId: String, atNanos: Long, coalesced: Boolean)

  private final case class CandidateCompletion(receipt: StateSyncRequestReceipt, failure: Option[Throwable] = None)

  private final case class PublishedCoverage(
      publicationId: String,
      generation: String,
      sinkTopology: Long,
      capturedMutationGeneration: Long,
      archiveSha256: String,
      archiveBytes: Long
  )

  private final case class CandidatePassResult(
      publication: Option[SnapshotPublication],
      noState: Boolean
  )

  private final class BackupState {
    val passLock                    = new ReentrantLock(true)
    val requested                   = new AtomicBoolean(false)
    val running                     = new AtomicBoolean(false)
    private val requests            = new ConcurrentLinkedQueue[AsyncRequest]()
    private val synchronousMonitor  = new Object
    private val synchronousRequests = scala.collection.mutable.ArrayBuffer.empty[SynchronousRequest]
    private val candidateRequests   = scala.collection.mutable.ArrayBuffer.empty[SynchronousCandidateRequest]
    private var synchronousRunning  = false
    private var candidateRunning    = false
    private var submittedRequests   = 0L
    private var publishedCoverage   = Option.empty[PublishedCoverage]

    def synchronousRequestCount: Long = synchronousMonitor.synchronized(submittedRequests)

    def candidateIdle: Boolean = synchronousMonitor.synchronized(candidateRequests.isEmpty && !candidateRunning)

    def invalidatePublishedCoverage(): Unit = synchronousMonitor.synchronized { publishedCoverage = None }

    def submitCandidate(request: SynchronousCandidateRequest): Unit = synchronousMonitor.synchronized {
      submittedRequests += 1L
      if (!request.coverageEligible) publishedCoverage = None
      candidateRequests += request
      synchronousMonitor.notifyAll()
    }

    def backupSynchronous(request: BackupRequest)(pass: Seq[BackupRequest] => Unit): Unit = {
      val completion = new CompletableFuture[Unit]()
      val submitted  = SynchronousRequest(request, completion)
      synchronousMonitor.synchronized {
        submittedRequests += 1L
        synchronousRequests += submitted
      }
      try {
        while (!completion.isDone) {
          val batch = synchronousMonitor.synchronized {
            if (completion.isDone) Vector.empty[SynchronousRequest]
            else if (!synchronousRunning) {
              synchronousRunning = true
              val captured = synchronousRequests.toVector
              synchronousRequests.clear()
              captured
            } else {
              synchronousMonitor.wait()
              Vector.empty[SynchronousRequest]
            }
          }
          if (batch.nonEmpty) {
            var failure: Throwable = null
            try pass(batch.map(_.request))
            catch { case error: Throwable => failure = error }
            finally
              synchronousMonitor.synchronized {
                batch.foreach { waiting =>
                  if (failure == null) waiting.completion.complete(())
                  else waiting.completion.completeExceptionally(failure)
                }
                synchronousRunning = false
                synchronousMonitor.notifyAll()
              }
          }
        }
        try completion.get()
        catch { case error: java.util.concurrent.ExecutionException => throw error.getCause }
      } catch {
        case interrupted: InterruptedException =>
          synchronousMonitor.synchronized {
            synchronousRequests -= submitted
          }
          Thread.currentThread().interrupt()
          throw interrupted
      }
    }

    def awaitCandidate(
        request: SynchronousCandidateRequest,
        uri: String,
        root: java.nio.file.Path,
        pass: BackupPassTrace => CandidatePassResult,
        recordReceipt: StateSyncRequestReceipt => Unit
    ): Unit = {
      def finish(
          req: SynchronousCandidateRequest,
          result: CandidateCompletion,
          trace: Option[BackupPassTrace] = None,
          coveredBy: Option[PublishedCoverage] = None
      ): Unit = if (!req.completion.isDone) {
        observeCandidate(req, result.receipt, result.failure, trace, coveredBy)
        req.completion.complete(result)
        try recordReceipt(result.receipt)
        finally req.receiptRecorded.complete(())
      }

      def deliver(): Unit = if (request.request.captureTelemetry) {
        val (evidence, publications) = synchronousMonitor.synchronized((request.evidence, request.publications))
        emitBackupEvidenceBestEffort {
          val backup = request.request
          backup.span.filter(_.stateSyncEvidenceEnabled) match {
            case Some(span) => span.recordStateSyncEvidence(evidence, publications)
            case None =>
              if (publications.isEmpty) OpenIvmExecutionSpan.emitRunStateSyncRequests(evidence, backup.receiptWriter)
              else {
                publications.foreach { publication =>
                  OpenIvmExecutionSpan.emitRunStateSyncPublication(
                    evidence.filter(_.publicationId == publication.publicationId),
                    publication,
                    backup.receiptWriter
                  )
                }
                val ids = publications.iterator.map(_.publicationId).toSet
                OpenIvmExecutionSpan.emitRunStateSyncRequests(
                  evidence.filterNot(item => ids.contains(item.publicationId)),
                  backup.receiptWriter
                )
              }
          }
        }
      }

      var completed: CandidateCompletion = null
      try {
        while (!request.completion.isDone) {
          val destination = destinationLock(uri)
          destination.lockInterruptibly()
          var unclaimedPublication = Option.empty[() => Unit]
          try {
            var batch = Vector.empty[SynchronousCandidateRequest]
            // Fence the cached ACK against worker registration, without holding the fence during capture/I/O.
            val passSinkTopology = BoundedTelemetryRuntime.withSinkTopology { topology =>
              OpenIvmStateBarrier.observeRuntimeTelemetrySinks(root)
              synchronousMonitor.synchronized {
                candidateRequests --= candidateRequests.filter(_.completion.isDone)
                if (!request.completion.isDone && !candidateRunning) {
                  if (!sharedStateSyncUris.containsKey(uri) && !OpenIvmStateBarrier.hasUntrackedSink(root)) {
                    publishedCoverage = publishedCoverage.filter(_.sinkTopology == topology)
                    publishedCoverage.foreach { coverage =>
                      val reusable = candidateRequests
                        .filter(req =>
                          req.coverageEligible && req.requestedMutationGeneration <= coverage.capturedMutationGeneration
                        )
                        .toVector
                      candidateRequests --= reusable
                      reusable.foreach { req =>
                        finish(
                          req,
                          CandidateCompletion(
                            StateSyncRequestReceipt(
                              req.requestId,
                              req.requestedMutationGeneration,
                              Some(coverage.capturedMutationGeneration),
                              Some(coverage.generation),
                              published = false,
                              reused = true,
                              coverageProven = true,
                              acknowledged = true,
                              outcome = "reused_committed_coverage",
                              archiveSha256 = Some(coverage.archiveSha256),
                              archiveBytes = Some(coverage.archiveBytes)
                            )
                          ),
                          coveredBy = Some(coverage)
                        )
                      }
                    }
                  } else publishedCoverage = None
                  if (!request.completion.isDone && candidateRequests.nonEmpty) {
                    candidateRunning = true
                    batch = candidateRequests.toVector
                    candidateRequests.clear()
                  }
                }
              }
              topology
            }

            if (batch.nonEmpty) {
              val trace = new BackupPassTrace(batch.map(_.request), "synchronous", UUID.randomUUID().toString)
              val admission = CandidateAdmission(trace.publicationId, System.nanoTime(), batch.size > 1)
              synchronousMonitor.synchronized { batch.foreach(_.admission = Some(admission)) }
              var result: CandidatePassResult = null
              var failure: Throwable          = null
              try {
                result = pass(trace)
                val hook = afterCandidatePublicationHookForTesting
                if (result.publication.nonEmpty && hook != null) {
                  try hook()
                  catch {
                    case _: InterruptedException => Thread.currentThread().interrupt()
                  }
                }
              } catch { case error: Throwable => failure = error }
              val stableSinkTopology = BoundedTelemetryRuntime.withSinkTopology { topology =>
                OpenIvmStateBarrier.observeRuntimeTelemetrySinks(root)
                topology == passSinkTopology && !OpenIvmStateBarrier.hasUntrackedSink(root)
              }
              synchronousMonitor.synchronized {
                try {
                  if (trace.enabled) {
                    val publication = trace.publicationEvidence(
                      if (failure != null) spanStatus(failure)
                      else if (result.noState) "no_local_state"
                      else if (result.publication.isEmpty) "failed"
                      else "success",
                      System.nanoTime(),
                      Some(batch.map(_.requestedMutationGeneration).max)
                    )
                    val traced = batch.filter(req => req.request.captureTelemetry && !req.completion.isDone)
                    val leader = traced.find(_.request.span.exists(_.stateSyncPublicationSinkConfigured))
                      .orElse(traced.find(_.request.receiptWriter.nonEmpty))
                      .orElse(traced.headOption)
                    leader match {
                      case Some(req) => req.publications :+= publication
                      case None =>
                        val writer = batch.iterator.flatMap(_.request.receiptWriter).take(1).toList.headOption
                        unclaimedPublication = Some(() =>
                          OpenIvmExecutionSpan.emitRunStateSyncPublication(Seq.empty, publication, writer)
                        )
                    }
                  }
                  if (failure != null) {
                    publishedCoverage = None
                    batch.foreach { req =>
                      finish(
                        req,
                        CandidateCompletion(
                          StateSyncRequestReceipt(
                            req.requestId,
                            req.requestedMutationGeneration,
                            trace.capturedMutationGeneration,
                            trace.committedGeneration,
                            published = trace.wasPublished,
                            reused = false,
                            coverageProven = false,
                            acknowledged = false,
                            outcome = "publication_failed"
                          ),
                          Some(failure)
                        ),
                        trace = Some(trace)
                      )
                    }
                  } else if (result.noState) {
                    publishedCoverage = None
                    batch.foreach { req =>
                      finish(
                        req,
                        CandidateCompletion(
                          StateSyncRequestReceipt(
                            req.requestId,
                            req.requestedMutationGeneration,
                            None,
                            None,
                            published = false,
                            reused = false,
                            coverageProven = false,
                            acknowledged = true,
                            outcome = "no_local_state"
                          )
                        ),
                        trace = Some(trace)
                      )
                    }
                  } else if (result.publication.isEmpty) {
                    publishedCoverage = None
                    val error = new java.io.IOException(
                      "openivm state-sync: synchronous pass produced no committed publication"
                    )
                    batch.foreach { req =>
                      finish(
                        req,
                        CandidateCompletion(
                          StateSyncRequestReceipt(
                            req.requestId,
                            req.requestedMutationGeneration,
                            None,
                            None,
                            published = false,
                            reused = false,
                            coverageProven = false,
                            acknowledged = false,
                            outcome = "publication_missing"
                          ),
                          Some(error)
                        ),
                        trace = Some(trace)
                      )
                    }
                  } else {
                    val publication = result.publication.get
                    val captured    = publication.capturedMutationGeneration
                    val shared      = sharedStateSyncUris.containsKey(uri)
                    val eligible    = !shared && stableSinkTopology &&
                      batch.forall(_.coverageEligible) && captured.isDefined
                    publishedCoverage =
                      if (eligible)
                        Some(
                          PublishedCoverage(
                            trace.publicationId,
                            publication.generation,
                            passSinkTopology,
                            captured.get,
                            publication.archiveSha256,
                            publication.archiveBytes
                          )
                        )
                      else None
                    batch.foreach { req =>
                      val proof = !shared && stableSinkTopology && req.coverageEligible &&
                        captured.exists(_ >= req.requestedMutationGeneration)
                      if (shared || !stableSinkTopology || !req.coverageEligible || proof) {
                        finish(
                          req,
                          CandidateCompletion(
                            StateSyncRequestReceipt(
                              req.requestId,
                              req.requestedMutationGeneration,
                              captured,
                              Some(publication.generation),
                              published = true,
                              reused = false,
                              coverageProven = proof,
                              acknowledged = true,
                              outcome = if (proof) "published_covered_generation" else "published_fresh_fallback",
                              archiveSha256 = Some(publication.archiveSha256),
                              archiveBytes = Some(publication.archiveBytes)
                            )
                          ),
                          trace = Some(trace)
                        )
                      } else if (!req.completion.isDone) {
                        val receipt = StateSyncRequestReceipt(
                            req.requestId,
                            req.requestedMutationGeneration,
                            captured,
                            Some(publication.generation),
                            published = true,
                            reused = false,
                            coverageProven = false,
                            acknowledged = false,
                            outcome = "later_generation_requires_new_publication",
                            archiveSha256 = Some(publication.archiveSha256),
                            archiveBytes = Some(publication.archiveBytes)
                        )
                        observeCandidate(req, receipt, None, Some(trace), None)
                        recordReceipt(receipt)
                        req.admission = None
                        candidateRequests += req
                      }
                    }
                  }
                } finally {
                  candidateRunning = false
                  synchronousMonitor.notifyAll()
                }
              }
            }
          } finally destination.unlock()
          unclaimedPublication.foreach(emit => emitBackupEvidenceBestEffort(emit()))
        }
        try completed = request.completion.get()
        catch { case error: java.util.concurrent.ExecutionException => throw error.getCause }
      } catch {
        case interrupted: InterruptedException =>
          val cancelled = synchronousMonitor.synchronized {
            candidateRequests -= request
            if (!request.completion.completeExceptionally(interrupted)) false
            else {
              val receipt = StateSyncRequestReceipt(
                request.requestId,
                request.requestedMutationGeneration,
                None,
                None,
                published = false,
                reused = false,
                coverageProven = false,
                acknowledged = false,
                outcome = "waiter_cancelled"
              )
              observeCandidate(request, receipt, Some(interrupted), None, None)
              recordReceipt(receipt)
              true
            }
          }
          Thread.currentThread().interrupt()
          if (cancelled || request.completion.isCompletedExceptionally) {
            deliver()
            throw interrupted
          }
          completed = request.completion.join()
      }
      request.receiptRecorded.join()
      deliver()
      completed.failure.foreach(error => throw error)
    }

    def capture(request: BackupRequest, afterPublish: () => Unit): Unit =
      requests.add(AsyncRequest(request, afterPublish))

    def drainRequests(): Seq[AsyncRequest] = {
      val drained = scala.collection.mutable.ArrayBuffer.empty[AsyncRequest]
      var request = requests.poll()
      while (request != null) {
        drained += request
        request = requests.poll()
      }
      drained.toSeq
    }
  }

  private val MaxPrunedPerPublish = 25

  private val restoreStates = new ConcurrentHashMap[String, CompletableFuture[Unit]]()
  private val backupStates  = new ConcurrentHashMap[String, BackupState]()
  @volatile private var backupPassHookForTesting: (String, SparkSession, String) => Unit = null
  @volatile private var stateSyncKeyHookForTesting: (SparkSession, String) => String     = null
  @volatile private var stateSyncUriHookForTesting: SparkSession => Option[String]       = null
  @volatile private var beforeLocalFileCopyHookForTesting: File => Unit                  = null
  @volatile private var beforeCaptureHookForTesting: () => Unit                          = null
  @volatile private var afterCandidatePublicationHookForTesting: () => Unit              = null
  @volatile private var copyLocalFileHookForTesting: (FileSystem, File, Path) => Unit    = null
  @volatile private var beforeSnapshotCommitHookForTesting: () => Unit                   = null
  @volatile private var afterSnapshotCommitHookForTesting: () => Unit                    = null

  private final class BackupPassTrace(
      requests: Seq[BackupRequest],
      mode: String,
      val publicationId: String
  ) {
    val enabled: Boolean       = requests.exists(_.captureTelemetry)
    private val startedAtNanos = if (enabled) System.nanoTime() else 0L
    private val startedAtUtc   = if (enabled) Instant.now().toString else ""
    private val phases         = scala.collection.mutable.ArrayBuffer.empty[OpenIvmExecutionSpan.StateSyncPhaseEvidence]
    private var admittedAtNanos       = Option.empty[Long]
    private var captured              = false
    private var published             = false
    private var capturedGeneration    = Option.empty[Long]

    def wasCaptured: Boolean = captured
    def wasPublished: Boolean = published
    def capturedMutationGeneration: Option[Long] = capturedGeneration
    def committedGeneration: Option[String] = if (published) snapshotGeneration else None
    def admittedAt: Option[Long] = admittedAtNanos
    private var snapshotGeneration    = Option.empty[String]
    private var capturedDatabaseCount = Option.empty[Long]
    private var capturedFileCount     = Option.empty[Long]
    private var capturedBytes         = Option.empty[Long]
    private var archiveBytes          = Option.empty[Long]
    private var uploadedBytes         = Option.empty[Long]
    private var prunedPaths           = Option.empty[Long]

    def markAdmitted(atNanos: Long): Unit = if (enabled) admittedAtNanos = Some(atNanos)

    def setSnapshotGeneration(generation: String): Unit = snapshotGeneration = Some(generation)

    def beginPhase(operation: String, workClass: String): StateSyncPhaseStart =
      if (enabled) StateSyncPhaseStart(operation, workClass, System.nanoTime(), Instant.now().toString)
      else StateSyncPhaseStart(operation, workClass, 0L, "")

    def endPhase(
        started: StateSyncPhaseStart,
        status: String,
        counts: Map[String, Long] = Map.empty,
        error: Option[Throwable] = None
    ): Unit = if (enabled) {
      val endedAt = System.nanoTime()
      phases += OpenIvmExecutionSpan.StateSyncPhaseEvidence(
        started.operation,
        status,
        started.workClass,
        started.startNanos,
        elapsedNanos(started.startNanos, endedAt),
        started.startUtc,
        counts,
        error.map(_.getClass.getSimpleName.take(128))
      )
    }

    def measure[A](operation: String, workClass: String)(body: => A): A = {
      if (!enabled) return body
      val started = beginPhase(operation, workClass)
      try {
        val result = body
        endPhase(started, "success")
        result
      } catch {
        case error: Throwable =>
          endPhase(started, spanStatus(error), error = Some(error))
          throw error
      }
    }

    def markCaptured(databaseCount: Int, generation: Option[Long]): Unit = {
      captured = true
      capturedGeneration = generation
      if (enabled) capturedDatabaseCount = Some(databaseCount.toLong)
    }

    def markCapturedInventory(fileCount: Int, bytes: Option[Long]): Unit = if (enabled) {
      capturedFileCount = Some(fileCount.toLong)
      capturedBytes = bytes
    }

    def markArchive(bytes: Long): Unit = if (enabled) archiveBytes = Some(bytes)

    def markUploaded(bytes: Long): Unit = if (enabled) uploadedBytes = Some(bytes)

    def markPublished(): Unit = published = true

    def markPruned(paths: Long): Unit = if (enabled) prunedPaths = Some(paths)

    def finish(status: String, endNanos: Long, error: Option[Throwable]): Unit = {
      if (!enabled) return
      val admittedAt          = admittedAtNanos.getOrElse(endNanos)
      val trackedRequests     = requests.filter(_.captureTelemetry)
      val requestEvidence = trackedRequests.map { request =>
        val queueStatus = if (admittedAtNanos.nonEmpty) "success" else spanStatus(error.orNull)
        OpenIvmExecutionSpan.StateSyncRequestEvidence(
          requestId = request.requestId,
          parentRequestId = request.parent.requestId,
          parentOperationId = request.parent.operationId,
          publicationId = publicationId,
          effectiveMode = request.mode,
          scope = if (request.parent.modelUniqueId.nonEmpty) "model" else "run",
          queueWaitNs = elapsedNanos(request.requestedAtNanos, admittedAt),
          status = queueStatus,
          requested = true,
          captured = wasCaptured,
          published = wasPublished,
          snapshotGenerationId = committedGeneration,
          acknowledgedSnapshotGenerationId =
            if (request.mode == "synchronous" && status == "success" && wasPublished) committedGeneration else None,
          requestStartMonotonicNs = request.requestedAtNanos,
          requestStartUtc = request.requestedAtUtc,
          capturedMutationGeneration = capturedMutationGeneration,
          publishedMutationGeneration = if (wasPublished) capturedMutationGeneration else None,
          coalesced = requests.size > 1
        )
      }
      emitEvidence(trackedRequests, requestEvidence, publicationEvidence(status, endNanos, None))
    }

    def publicationEvidence(
        status: String,
        endNanos: Long,
        requestedMutationGeneration: Option[Long]
    ): OpenIvmExecutionSpan.StateSyncPublicationEvidence = {
      val trackedRequests = requests.filter(_.captureTelemetry)
      val counts = Map(
        "requested_requests" -> Some(requests.size.toLong),
        "captured_requests"  -> Some(if (wasCaptured) requests.size.toLong else 0L),
        "published_requests" -> Some(if (wasPublished) requests.size.toLong else 0L),
        "captured_databases" -> capturedDatabaseCount,
        "captured_files"     -> capturedFileCount,
        "captured_bytes"     -> capturedBytes,
        "archive_bytes"      -> archiveBytes,
        "uploaded_bytes"     -> uploadedBytes,
        "pruned_paths"       -> prunedPaths
      )
      OpenIvmExecutionSpan.StateSyncPublicationEvidence(
        publicationId = publicationId,
        effectiveMode = mode,
        status = status,
        startMonotonicNs = startedAtNanos,
        durationNs = elapsedNanos(startedAtNanos, endNanos),
        startUtc = startedAtUtc,
        phases = phases.toVector,
        counts = counts,
        requestedRequests = requests.size,
        capturedRequests = if (wasCaptured) requests.size else 0,
        publishedRequests = if (wasPublished) requests.size else 0,
        captured = wasCaptured,
        published = wasPublished,
        snapshotGenerationId = committedGeneration,
        requestIds = trackedRequests.map(_.requestId).take(OpenIvmExecutionSpan.MaxStateSyncRequests),
        parentRequestIds =
          trackedRequests.flatMap(_.parent.requestId).distinct.take(OpenIvmExecutionSpan.MaxStateSyncRequests),
        droppedRequestIds = math.max(0, trackedRequests.size - OpenIvmExecutionSpan.MaxStateSyncRequests).toLong,
        droppedParentRequestIds = math
          .max(
            0,
            trackedRequests.flatMap(_.parent.requestId).distinct.size - OpenIvmExecutionSpan.MaxStateSyncRequests
          )
          .toLong,
        // A pass's requested generation is the highest requested generation in its admitted batch.
        requestedMutationGeneration = requestedMutationGeneration,
        capturedMutationGeneration = capturedMutationGeneration,
        publishedMutationGeneration = if (wasPublished) capturedMutationGeneration else None
      )
    }

    private def emitEvidence(
        trackedRequests: Seq[BackupRequest],
        requestEvidence: Seq[OpenIvmExecutionSpan.StateSyncRequestEvidence],
        publication: OpenIvmExecutionSpan.StateSyncPublicationEvidence
    ): Unit = {
      val parented = trackedRequests.zip(requestEvidence).flatMap { case (request, evidence) =>
        request.span.filter(_.stateSyncEvidenceEnabled).map(_ -> evidence)
      }
      val parentedRequests     = parented.map(_._2.requestId).toSet
      val unparented           = requestEvidence.filterNot(request => parentedRequests.contains(request.requestId))
      val unparentedRequestIds = unparented.iterator.map(_.requestId).toSet
      val unparentedReceiptWriter = trackedRequests
        .filter(request => unparentedRequestIds.contains(request.requestId))
        .iterator
        .flatMap(_.receiptWriter)
        .take(1)
        .toList
        .headOption
      val sinkBackedLeader    = parented.find(_._1.stateSyncPublicationSinkConfigured).map(_._1)
      val publishToRunReceipt = parented.nonEmpty && sinkBackedLeader.isEmpty && unparentedReceiptWriter.nonEmpty
      val leader = sinkBackedLeader
        .orElse(if (publishToRunReceipt) None else parented.headOption.map(_._1))
      if (parented.isEmpty)
        OpenIvmExecutionSpan.emitRunStateSyncPublication(
          requestEvidence,
          publication,
          trackedRequests.iterator.flatMap(_.receiptWriter).take(1).toList.headOption
        )
      else {
        parented.groupBy(_._1).foreach { case (parent, values) =>
          parent.recordStateSyncEvidence(
            values.map(_._2),
            if (leader.contains(parent)) Seq(publication) else Seq.empty
          )
        }
        if (publishToRunReceipt)
          OpenIvmExecutionSpan.emitRunStateSyncPublication(requestEvidence, publication, unparentedReceiptWriter)
        else if (unparented.nonEmpty)
          OpenIvmExecutionSpan.emitRunStateSyncRequests(unparented, unparentedReceiptWriter)
      }
    }
  }

  private def emitBackupEvidenceBestEffort(body: => Unit): Unit = {
    // A cancelled waiter or a post-commit interruption still gets a receipt.
    val interrupted = Thread.interrupted()
    try body
    catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
      case NonFatal(error) => log.warn(s"openivm state-sync: optional telemetry emission failed: $error")
    } finally if (interrupted) Thread.currentThread().interrupt()
  }

  private def observeCandidate(
      candidate: SynchronousCandidateRequest,
      receipt: StateSyncRequestReceipt,
      failure: Option[Throwable],
      trace: Option[BackupPassTrace],
      coveredBy: Option[PublishedCoverage]
  ): Unit = {
    val request = candidate.request
    if (!request.captureTelemetry) return
    val admission = candidate.admission
    val now = System.nanoTime()
    candidate.evidence :+= OpenIvmExecutionSpan.StateSyncRequestEvidence(
      requestId = request.requestId,
      parentRequestId = request.parent.requestId,
      parentOperationId = request.parent.operationId,
      publicationId = admission.map(_.publicationId).orElse(coveredBy.map(_.publicationId)).getOrElse(""),
      effectiveMode = request.mode,
      scope = if (request.parent.modelUniqueId.nonEmpty) "model" else "run",
      queueWaitNs = elapsedNanos(request.requestedAtNanos, trace.flatMap(_.admittedAt).orElse(admission.map(_.atNanos)).getOrElse(now)),
      status = failure.fold("success")(spanStatus),
      requested = true,
      captured = trace.exists(_.wasCaptured),
      published = receipt.published,
      snapshotGenerationId = receipt.publicationGeneration,
      acknowledgedSnapshotGenerationId = receipt.publicationGeneration.filter(_ => receipt.acknowledged),
      requestStartMonotonicNs = request.requestedAtNanos,
      requestStartUtc = request.requestedAtUtc,
      requestedMutationGeneration = Some(receipt.requestedMutationGeneration),
      capturedMutationGeneration = trace.flatMap(_.capturedMutationGeneration),
      publishedMutationGeneration = coveredBy.map(_.capturedMutationGeneration)
        .orElse(trace.filter(_.wasPublished).flatMap(_.capturedMutationGeneration)),
      reused = receipt.reused,
      coalesced = admission.exists(_.coalesced),
      coverageProven = receipt.coverageProven,
      outcome = Some(receipt.outcome)
    )
  }

  private def elapsedNanos(startNanos: Long, endNanos: Long): Long = {
    val elapsed = endNanos - startNanos
    require(elapsed >= 0L, "state-sync duration must be non-negative")
    elapsed
  }

  private def spanStatus(error: Throwable): String =
    if (error == null) "failed"
    else if (error.isInstanceOf[InterruptedException] || error.isInstanceOf[java.io.InterruptedIOException]) "cancelled"
    else "failed"

  private def newBackupRequest(spark: SparkSession, mode: String): BackupRequest = {
    val span             = OpenIvmExecutionSpan.captureCurrent()
    val captureTelemetry = OpenIvmExecutionSpan.stateSyncCaptureEnabled(spark, span)
    val emptyParent = OpenIvmExecutionSpan.StateSyncParentContext(
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    val parent =
      if (!captureTelemetry) emptyParent
      else
        try OpenIvmExecutionSpan.captureStateSyncParent(spark, span)
        catch {
          case NonFatal(error) =>
            log.warn(s"openivm state-sync: optional telemetry context capture failed: $error")
            emptyParent
        }
    BackupRequest(
      requestId = if (captureTelemetry) UUID.randomUUID().toString else "",
      parent = parent,
      span = span,
      requestedAtNanos = if (captureTelemetry) System.nanoTime() else 0L,
      requestedAtUtc = if (captureTelemetry) Instant.now().toString else "",
      mode = mode,
      captureTelemetry = captureTelemetry,
      receiptWriter = if (captureTelemetry) OpenIvmExecutionSpan.captureStateSyncReceiptWriter(spark, span) else None
    )
  }

  private val destinationLocks = new ConcurrentHashMap[String, ReentrantLock]()
  private def destinationLock(uri: String): ReentrantLock =
    destinationLocks.computeIfAbsent(uri, _ => new ReentrantLock(true))
  private val stateSyncRootsByUri = new ConcurrentHashMap[String, ConcurrentHashMap[String, java.lang.Boolean]]()
  private val sharedStateSyncUris = new ConcurrentHashMap[String, java.lang.Boolean]()
  @volatile private var stateSyncRequestReceiptHookForTesting: StateSyncRequestReceipt => Unit = null
  private val backupExecutor = Executors.newCachedThreadPool { r =>
    val t = new Thread(r, "openivm-state-sync")
    t.setDaemon(true)
    t
  }

  private def localRoot(spark: SparkSession): File =
    OpenIvmStateBarrier.root(spark).toFile

  private def hasLocalStateFiles(root: File): Boolean = {
    if (!root.exists()) return false
    val paths = Files.walk(root.toPath)
    try {
      val iterator = paths.iterator()
      var found    = false
      while (iterator.hasNext && !found) {
        found = Files.isRegularFile(iterator.next())
      }
      found
    } finally paths.close()
  }

  private def canonicalLocalRoot(spark: SparkSession): String =
    try localRoot(spark).getCanonicalFile.getAbsolutePath
    catch {
      case NonFatal(_) => localRoot(spark).getAbsolutePath
    }

  private def stateSyncKey(spark: SparkSession, uri: String): String =
    s"${canonicalLocalRoot(spark)}|$uri"

  private def effectiveStateSyncKey(spark: SparkSession, uri: String): String = {
    val hook = stateSyncKeyHookForTesting
    if (hook == null) stateSyncKey(spark, uri) else hook(spark, uri)
  }

  private def configuredStateSyncUri(spark: SparkSession): Option[String] = {
    val hook = stateSyncUriHookForTesting
    if (hook == null) FeatureGate.stateSyncUri(spark) else hook(spark)
  }

  private def stateSyncUri(spark: SparkSession): Option[String] =
    configuredStateSyncUri(spark).map { value =>
      val path = new Path(value)
      if (backupPassHookForTesting != null) path.toString
      else {
        val fs        = path.getFileSystem(spark.sessionState.newHadoopConf())
        val qualified = fs.makeQualified(path).toUri.normalize()
        // File.toURI appends a trailing slash only after the directory exists. Build the
        // identity from the canonical path so mkdirs cannot split locks or cached coverage.
        if ("file".equalsIgnoreCase(qualified.getScheme))
          new Path("file", null, new File(qualified).getCanonicalPath).toString
        else new Path(qualified).toString
      }
    }

  private[rocksdb] def guardedUri(spark: SparkSession): Option[String] =
    spark.sparkContext.getConf.getOption(FeatureGate.GuardedStateSyncUriKey)

  private[openivm] def guardedSqlEnabled(spark: SparkSession): Boolean = guardedUri(spark).isDefined

  private def checkGuardedSettings(spark: SparkSession): Unit = {
    if (
      spark.sparkContext.isStopped || configuredStateSyncUri(spark).nonEmpty ||
      spark.conf.getOption(FeatureGate.StateSyncUriKey).exists(_.trim.nonEmpty) ||
      FeatureGate.stateSyncAsyncEnabled(spark) || FeatureGate.gcEnabled(spark)
    )
      throw new java.io.IOException(
        "guarded SQL state sync requires an active Spark application, no legacy URI, synchronous state sync, and legacy GC disabled"
      )
  }

  private[rocksdb] def attachGuarded(
      spark: SparkSession,
      store: GuardedSnapshotDecisions.ArchiveStore
  ): Unit = {
    checkGuardedSettings(spark)
    if (!guardedUri(spark).contains(store.canonicalRoot))
      throw new java.io.IOException("guarded SQL checkpoint root does not match the startup configuration")
    val binding = new GuardedBinding(canonicalLocalRoot(spark), store)
    guardedBindings.synchronized {
      if (guardedBindings.containsKey(spark.sparkContext))
        throw new java.io.IOException("guarded SQL checkpoint transport is already attached")
      guardedBindings.put(spark.sparkContext, binding)
    }
  }

  private def guardedBinding(spark: SparkSession): Option[GuardedBinding] = guardedUri(spark).map { uri =>
    checkGuardedSettings(spark)
    val binding = guardedBindings
      .synchronized(Option(guardedBindings.get(spark.sparkContext)))
      .getOrElse(throw new java.io.IOException("guarded SQL checkpoint transport was not attached at startup"))
    if (binding.store.canonicalRoot != uri || binding.root != canonicalLocalRoot(spark))
      throw new java.io.IOException("guarded SQL checkpoint binding changed")
    binding
  }

  private def restoreGuardedOnce(spark: SparkSession, binding: GuardedBinding): Unit = {
    if (binding.restoring.compareAndSet(false, true)) {
      try {
        binding.latest = restoreGuarded(spark, binding.store)
        binding.restored.complete(())
      } catch {
        case error: Throwable =>
          binding.restored.completeExceptionally(error)
          throw error
      }
    } else {
      try binding.restored.get()
      catch { case error: java.util.concurrent.ExecutionException => throw error.getCause }
    }
  }

  private def recordStateSyncReceipt(receipt: StateSyncRequestReceipt): Unit = {
    log.info(
      s"openivm state-sync: sync_request request_id=${receipt.requestId} " +
        s"requested_generation=${receipt.requestedMutationGeneration} " +
        s"captured_generation=${receipt.capturedMutationGeneration.getOrElse("none")} " +
        s"publication_generation=${receipt.publicationGeneration.getOrElse("none")} " +
        s"published=${receipt.published} reused=${receipt.reused} " +
        s"archive_sha256=${receipt.archiveSha256.getOrElse("none")} " +
        s"archive_bytes=${receipt.archiveBytes.getOrElse(0L)} " +
        s"coverage_proven=${receipt.coverageProven} acknowledged=${receipt.acknowledged} " +
        s"outcome=${receipt.outcome}"
    )
    val hook = stateSyncRequestReceiptHookForTesting
    if (hook != null)
      try hook(receipt)
      catch { case NonFatal(error) => log.warn(s"openivm state-sync: receipt hook failed: $error") }
  }

  private def registerRootForUri(uri: String, localRoot: String): Boolean = {
    val roots = stateSyncRootsByUri.computeIfAbsent(uri, _ => new ConcurrentHashMap[String, java.lang.Boolean]())
    roots.putIfAbsent(localRoot, java.lang.Boolean.TRUE)
    if (roots.size() > 1 && sharedStateSyncUris.putIfAbsent(uri, java.lang.Boolean.TRUE) == null) {
      val states = backupStates.entrySet().iterator()
      while (states.hasNext) {
        val entry = states.next()
        if (entry.getKey.endsWith(s"|$uri")) entry.getValue.invalidatePublishedCoverage()
      }
    }
    sharedStateSyncUris.containsKey(uri)
  }

  private def backupStateFor(key: String): BackupState = {
    val fresh    = new BackupState
    val existing = backupStates.putIfAbsent(key, fresh)
    if (existing != null) existing else fresh
  }

  /** Download the remote state tree into the local `_openivm` dir on the first
    * call per Spark application. Restore failures fail all callers closed. */
  def maybeRestore(spark: SparkSession): Unit = {
    guardedBinding(spark) match {
      case Some(binding) =>
        restoreGuardedOnce(spark, binding)
        return
      case None => ()
    }
    val uri = configuredStateSyncUri(spark)
      .getOrElse(
        return
      )
    OpenIvmStateBarrier.register(spark)
    val key    = s"${spark.sparkContext.applicationId}|${effectiveStateSyncKey(spark, uri)}"
    val waiter = new CompletableFuture[Unit]()
    val prior  = restoreStates.putIfAbsent(key, waiter)
    if (prior != null) {
      try prior.join()
      catch { case error: java.util.concurrent.CompletionException => throw error.getCause }
      return
    }

    try {
      OpenIvmStateBarrier.withCapture(spark) {
        restoreNow(spark, uri)
      }
      waiter.complete(())
    } catch {
      case error: Throwable =>
        waiter.completeExceptionally(error)
        throw error
    }
  }

  /** Coalesced, non-blocking incremental backup of the local `_openivm` tree to
    * the remote URI. If a backup is already queued this call is folded into that
    * in-flight work and forces one more pass afterwards. */
  def backupAsync(spark: SparkSession): Unit = backupAsync(spark, () => ())

  /** Select the command's backup policy, then run cleanup only after publication succeeds. */
  def backupForCommand(spark: SparkSession, afterPublish: () => Unit = () => ()): Unit =
    if (FeatureGate.stateSyncAsyncEnabled(spark)) backupAsync(spark, afterPublish)
    else {
      backupNow(spark)
      afterPublish()
    }

  private def backupAsync(spark: SparkSession, afterPublish: () => Unit): Unit = {
    if (guardedSqlEnabled(spark))
      throw new java.io.IOException("guarded SQL checkpoint publication cannot be asynchronous")
    val uri = stateSyncUri(spark)
      .getOrElse {
        afterPublish()
        return
      }
    val key = effectiveStateSyncKey(spark, uri)
    registerRootForUri(uri, canonicalLocalRoot(spark))
    val state = backupStateFor(key)
    val request = newBackupRequest(spark, "asynchronous")
    if (FeatureGate.stateSyncCheckpointEnabled(spark)) {
      val root = OpenIvmStateBarrier.root(spark)
      OpenIvmStateBarrier.observeTelemetrySinks(spark, root)
      if (!request.span.forall(_.stateSyncSinksDisjoint(root))) OpenIvmStateBarrier.markUntrackedSink(root)
    }
    state.capture(request, afterPublish)
    state.requested.set(true)
    scheduleBackupIfNeeded(state, spark, uri)
  }

  private def scheduleBackupIfNeeded(state: BackupState, spark: SparkSession, uri: String): Unit =
    if (state.running.compareAndSet(false, true)) {
      val key = effectiveStateSyncKey(spark, uri)
      backupExecutor.execute(() => runBackupLoop(key, state, spark, uri))
    }

  private def runBackupLoop(key: String, state: BackupState, spark: SparkSession, uri: String): Unit =
    try {
      var continue = true
      while (continue) {
        state.requested.set(false)
        // Capture before the snapshot: a later request must not clean up files
        // until a pass that covers its committed state has succeeded.
        val requests  = state.drainRequests()
        val started   = System.nanoTime()
        var succeeded = false
        try {
          runBackupPass(key, spark, uri, requests.map(_.request), "asynchronous")
          succeeded = true
        } catch { case NonFatal(e) => log.warn(s"openivm state-sync: backup failed: $e") }
        finally {
          val durationMs = (System.nanoTime() - started) / 1000000L
          requests.foreach(_.request.span.foreach(_.recordRocksDbBackup(durationMs)))
          log.info(
            s"openivm state-sync: async backup duration_ms=$durationMs succeeded=$succeeded requests=${requests.size}"
          )
        }
        if (succeeded)
          requests.foreach { request =>
            try request.afterPublish()
            catch { case NonFatal(e) => log.warn(s"openivm state-sync: post-backup cleanup failed: $e") }
          }
        continue = state.requested.get()
      }
    } finally {
      state.running.set(false)
      if (state.requested.get() && state.running.compareAndSet(false, true)) {
        backupExecutor.execute(() => runBackupLoop(key, state, spark, uri))
      }
    }

  /** Synchronously publish a snapshot covering this request's committed state. */
  def backupNow(spark: SparkSession): Unit = {
    guardedBinding(spark) match {
      case Some(binding) =>
        restoreGuardedOnce(spark, binding)
        val span    = OpenIvmExecutionSpan.captureCurrent()
        val started = System.nanoTime()
        try
          binding.backup.backupSynchronous(newBackupRequest(spark, "synchronous")) { requests =>
            val previous = new GuardedSnapshotDecisions(binding.store).latestPublished().map(_.generation)
            if (previous != binding.latest)
              throw new java.io.IOException(
                "guarded SQL checkpoint was published by another writer; fresh restore required"
              )
            val previousStamp = previous.map(_.value.take(19).toLong).getOrElse(0L)
            val stamp         = math.max(System.currentTimeMillis(), Math.addExact(previousStamp, 1L))
            val generation    = GuardedSnapshotDecisions.Generation(f"$stamp%019d-${UUID.randomUUID()}")
            val trace         = new BackupPassTrace(requests, "synchronous", UUID.randomUUID().toString)
            trace.markAdmitted(System.nanoTime())
            trace.setSnapshotGeneration(generation.value)
            var failure = Option.empty[Throwable]
            try {
              publishGuardedWithTrace(spark, generation, binding.store, trace)
              binding.latest = Some(generation)
            } catch {
              case error: Throwable =>
                failure = Some(error)
                throw error
            } finally {
              try trace.finish(failure.fold("success")(_ => spanStatus(failure.get)), System.nanoTime(), failure)
              catch {
                case NonFatal(error) => log.warn(s"openivm state-sync: optional telemetry emission failed: $error")
              }
            }
          }
        finally span.foreach(_.recordRocksDbBackup((System.nanoTime() - started) / 1000000L))
        return
      case None => ()
    }
    val uri = stateSyncUri(spark)
      .getOrElse(
        return
      )
    val key               = effectiveStateSyncKey(spark, uri)
    val sharedDestination = registerRootForUri(uri, canonicalLocalRoot(spark))
    val request           = newBackupRequest(spark, "synchronous")
    val started           = System.nanoTime()
    try {
      val state = backupStateFor(key)
      if (!FeatureGate.stateSyncCheckpointEnabled(spark)) {
        state.backupSynchronous(request) { requests =>
          runBackupPass(key, spark, uri, requests, "synchronous")
        }
      } else {
        val requestId = Option(spark.sparkContext.getLocalProperty("openivm.request_id"))
          .filter(_.nonEmpty)
          .getOrElse(UUID.randomUUID().toString)
        val root = OpenIvmStateBarrier.root(spark)
        if (!request.span.forall(_.stateSyncSinksDisjoint(root))) OpenIvmStateBarrier.markUntrackedSink(root)
        val coverageEligible = OpenIvmStateBarrier.mutationCoverageEnabled(spark) && !sharedDestination
        var candidate: SynchronousCandidateRequest = null
        OpenIvmStateBarrier.withCurrentMutationGeneration(spark) { requestedGeneration =>
          candidate = SynchronousCandidateRequest(
            requestId,
            request,
            requestedGeneration,
            coverageEligible,
            new CompletableFuture[CandidateCompletion](),
            new CompletableFuture[Unit]()
          )
          state.submitCandidate(candidate)
        }
        state.awaitCandidate(candidate, uri, root, trace => runCandidateBackupPass(key, spark, uri, trace), recordStateSyncReceipt)
      }
    } finally {
      val durationMs = (System.nanoTime() - started) / 1000000L
      request.span.foreach(_.recordRocksDbBackup(durationMs))
    }
  }

  private def runBackupPass(
      key: String,
      spark: SparkSession,
      uri: String,
      requests: Seq[BackupRequest],
      mode: String
  ): Unit = {
    val operationId       = if (requests.exists(_.captureTelemetry)) UUID.randomUUID().toString else ""
    val trace             = new BackupPassTrace(requests, mode, operationId)
    val destination       = destinationLock(uri)
    val lock              = backupStateFor(key).passLock
    var destinationLocked = false
    var locked            = false
    var failure           = Option.empty[Throwable]
    try {
      destination.lockInterruptibly()
      destinationLocked = true
      backupStateFor(key).invalidatePublishedCoverage()
      lock.lockInterruptibly()
      locked = true
      trace.markAdmitted(System.nanoTime())
      val hook = backupPassHookForTesting
      if (hook == null) backupNowInternal(spark, uri, trace)
      else hook(key, spark, uri)
    } catch {
      case error: Throwable =>
        failure = Some(error)
        throw error
    } finally {
      val ended = if (trace.enabled) System.nanoTime() else 0L
      if (locked) lock.unlock()
      if (destinationLocked) destination.unlock()
      try trace.finish(failure.fold("success")(_ => spanStatus(failure.get)), ended, failure)
      catch {
        case NonFatal(telemetryError) =>
          log.warn(s"openivm state-sync: optional telemetry emission failed: $telemetryError")
      }
    }
  }

  private def runCandidateBackupPass(
      key: String,
      spark: SparkSession,
      uri: String,
      trace: BackupPassTrace
  ): CandidatePassResult = {
    val lock = backupStateFor(key).passLock
    lock.lockInterruptibly()
    try {
      trace.markAdmitted(System.nanoTime())
      val hook = backupPassHookForTesting
      if (hook != null) {
        hook(key, spark, uri)
        CandidatePassResult(None, noState = !localRoot(spark).exists())
      } else {
        val publication = backupNowInternal(spark, uri, trace)
        CandidatePassResult(publication, noState = publication.isEmpty && !localRoot(spark).exists())
      }
    } finally lock.unlock()
  }

  private def restoreNow(spark: SparkSession, uri: String): Unit = {
    val local = localRoot(spark)
    if (hasLocalStateFiles(local)) return

    val hconf      = spark.sessionState.newHadoopConf()
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(hconf)
    if (!fs.exists(remoteRoot)) {
      log.info(s"openivm state-sync: no remote state at $uri, starting fresh")
      return
    }

    val committed = latestCommit(fs, remoteRoot)
    committed.flatMap(_.archive).foreach { info =>
      restoreArchive(fs, committed.get.root, info, local)
      return
    }
    val snapshotRoot = committed.map(_.root).getOrElse(remoteRoot)
    local.mkdirs()
    var restored = 0
    val it       = fs.listFiles(snapshotRoot, /* recursive = */ true)
    while (it.hasNext) {
      val remotePath = it.next().getPath
      val rel        = relativize(snapshotRoot, remotePath)
      if (!rel.startsWith(".openivm-state-sync/")) {
        val dst = new File(local, rel)
        Option(dst.getParentFile).foreach(_.mkdirs())
        // useRawLocalFileSystem=true so Hadoop does NOT drop .crc sidecars into
        // the RocksDB directory (RocksDB would choke on the extra files).
        fs.copyToLocalFile(
          /* delSrc = */ false,
          remotePath,
          new Path(dst.getAbsolutePath), /* useRawLocalFileSystem = */ true
        )
        restored += 1
      }
    }
    if (restored == 0 && snapshotRoot == remoteRoot && fs.exists(new Path(remoteRoot, ".openivm-state-sync")))
      throw new java.io.IOException(s"openivm state-sync: no committed snapshot or legacy state at $uri")
    log.info(s"openivm state-sync: restored $restored files from $uri to $local")
  }

  private final case class CapturedState(databaseCount: Int, mutationGeneration: Option[Long])
  private final case class PackedArchive(checksum: String, archiveBytes: Long, mutationGeneration: Option[Long])
  private final case class PruneResult(deletedPaths: Long, failure: Option[Throwable])

  private def backupNowInternal(
      spark: SparkSession,
      uri: String,
      trace: BackupPassTrace
  ): Option[SnapshotPublication] = {
    val local = localRoot(spark)
    if (!local.exists()) return None

    val hconf         = spark.sessionState.newHadoopConf()
    val namespaceRoot = new Path(uri)
    val fs            = namespaceRoot.getFileSystem(hconf)
    val controlRoot   = new Path(namespaceRoot, ".openivm-state-sync")
    val commitRoot    = new Path(controlRoot, "commits")
    if (!fs.mkdirs(commitRoot))
      throw new java.io.IOException(s"openivm state-sync: cannot create commit directory $commitRoot")
    val previous = fs
      .listStatus(commitRoot)
      .iterator
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .map(_.getPath.getName.takeWhile(_ != '-').toLong)
      .foldLeft(0L)(math.max)
    val stamp      = math.max(System.currentTimeMillis(), previous + 1L)
    val generation = f"$stamp%019d-${UUID.randomUUID()}"
    val remoteRoot = new Path(new Path(controlRoot, "snapshots"), generation)
    if (!fs.mkdirs(remoteRoot))
      throw new java.io.IOException(s"openivm state-sync: cannot create snapshot $remoteRoot")
    trace.setSnapshotGeneration(generation)
    var publication = Option.empty[SnapshotPublication]
    withPackedSnapshot(spark, local, generation, trace, allowReuse = true) {
      (archive, archiveBytes, checksum, captured, onCommitted) =>
        publishLegacy(
          spark,
          uri,
          fs,
          controlRoot,
          commitRoot,
          remoteRoot,
          generation,
          archive,
          archiveBytes,
          checksum,
          trace,
          onCommitted
        )
        publication = Some(SnapshotPublication(generation, captured, checksum, archiveBytes))
    }
    publication
  }

  private def withPackedSnapshot(
      spark: SparkSession,
      local: File,
      generation: String,
      trace: BackupPassTrace,
      allowReuse: Boolean
  )(
      consume: (File, Long, String, Option[Long], () => Unit) => Unit
  ): Unit = {
    OpenIvmStateBarrier.register(spark)
    val archive   = new File(local.getParentFile, s".openivm-state-snapshot-$generation.zip")
    val capture   = Files.createTempDirectory(local.getParentFile.toPath, ".openivm-state-capture-").toFile
    val reuse     = allowReuse && FeatureGate.stateSyncCheckpointReuseEnabled(spark)
    var outcomes  = Vector.empty[(String, OpenIvmCheckpointReuse.Capture)]
    var published = false
    var capturedMutationGeneration = Option.empty[Long]
    try {
      val captureHook = beforeCaptureHookForTesting
      if (captureHook != null) captureHook()
      val barrierStarted  = trace.beginPhase("spark.backup.barrier_wait", "wait")
      var barrierAcquired = false
      try {
        OpenIvmStateBarrier.withCapture(spark) {
          trace.endPhase(barrierStarted, "success")
          barrierAcquired = true
          val captured = trace.measure("spark.backup.capture", "durability") {
            OpenIvmSnapshotArchive.checkInterrupted()
            val localBase = local.toPath
            val files     = backupFiles(local)
            val databases = files
              .filter(file => file.getName == "CURRENT" && file.getParentFile.getName == "rocksdb")
              .map(_.getParentFile.toPath)
              .distinct
              .sortBy(_.toString)
            val captures = OpenIvmStateBarrier.captureDatabases(
              spark,
              databases.iterator.map { db =>
                db.toFile.getCanonicalPath -> capture.toPath.resolve(localBase.relativize(db))
              }.toVector,
              reuse
            )
            if (reuse) outcomes = databases.map(_.toString).zip(captures).toVector
            files.filterNot(file => databases.exists(file.toPath.startsWith)).foreach { file =>
              OpenIvmSnapshotArchive.checkInterrupted()
              val target = capture.toPath.resolve(localBase.relativize(file.toPath))
              Files.createDirectories(target.getParent)
              Files.copy(file.toPath, target)
            }
            CapturedState(
              databases.size,
              if (FeatureGate.stateSyncCheckpointEnabled(spark))
                Some(OpenIvmStateBarrier.currentMutationGeneration(spark))
              else None
            )
          }
          capturedMutationGeneration = captured.mutationGeneration
          trace.markCaptured(captured.databaseCount, captured.mutationGeneration)
        }
      } catch {
        case error: Throwable =>
          if (!barrierAcquired) trace.endPhase(barrierStarted, spanStatus(error), error = Some(error))
          throw error
      }

      val packed = trace.measure("spark.backup.pack_hash", "durability") {
        val files = backupFiles(capture)
        if (trace.enabled) {
          val capturedBytes = files.foldLeft(Option(0L)) { (total, file) =>
            total.flatMap { bytes =>
              try Some(bytes + Files.size(file.toPath))
              catch { case NonFatal(_) => None }
            }
          }
          trace.markCapturedInventory(files.size, capturedBytes)
        }
        val checksum = OpenIvmSnapshotArchive.pack(
          capture,
          files,
          archive,
          file => {
            val hook = beforeLocalFileCopyHookForTesting
            if (hook != null) hook(file)
          }
        )
        val archiveBytes = archive.length()
        trace.markArchive(archiveBytes)
        PackedArchive(checksum, archiveBytes, capturedMutationGeneration)
      }
      val onCommitted = () => {
        published = true
        val afterCommitHook = afterSnapshotCommitHookForTesting
        if (afterCommitHook != null) afterCommitHook()
        if (reuse) outcomes.foreach { case (path, outcome) =>
          val digest = java.security.MessageDigest
            .getInstance("SHA-256")
            .digest(path.getBytes(StandardCharsets.UTF_8))
            .map(byte => f"${byte & 0xff}%02x")
            .mkString
          log.info(s"openivm state-sync: checkpoint generation=$generation store=$digest outcome=${outcome.outcome}")
        }
        if (reuse) outcomes.foreach(_._2.retain.foreach(_()))
      }
      consume(archive, packed.archiveBytes, packed.checksum, packed.mutationGeneration, onCommitted)
    } finally {
      if (reuse && !published)
        log.warn(s"openivm state-sync: checkpoint generation=$generation outcome=failed_publication")
      try Files.deleteIfExists(archive.toPath)
      finally deleteStagedTree(capture)
    }
  }

  private def publishLegacy(
      spark: SparkSession,
      uri: String,
      fs: FileSystem,
      controlRoot: Path,
      commitRoot: Path,
      remoteRoot: Path,
      generation: String,
      archive: File,
      archiveBytes: Long,
      checksum: String,
      trace: BackupPassTrace,
      onCommitted: () => Unit
  ): Unit = {
    val remoteArchive = new Path(remoteRoot, "snapshot.zip")
    val copyHook      = copyLocalFileHookForTesting
    trace.measure("spark.backup.upload", "durability") {
      if (copyHook != null) copyHook(fs, archive, remoteArchive)
      else fs.copyFromLocalFile(false, true, new Path(archive.getAbsolutePath), remoteArchive)
      OpenIvmSnapshotArchive.checkInterrupted()
      val remoteBytes = fs.getFileStatus(remoteArchive).getLen
      if (remoteBytes != archiveBytes)
        throw new java.io.IOException("openivm state-sync: uploaded archive length mismatch")
      trace.markUploaded(remoteBytes)
    }
    val pendingCommit = new Path(commitRoot, s".$generation.pending")
    val committed     = new Path(commitRoot, s"$generation.commit")
    trace.measure("spark.backup.commit", "durability") {
      val output = fs.create(pendingCommit, false)
      try
        output.write(
          s"OPENIVM_STATE_SNAPSHOT_V2\n$generation\n$archiveBytes\n${checksum}\n".getBytes(
            StandardCharsets.UTF_8
          )
        )
      finally output.close()
      val commitHook = beforeSnapshotCommitHookForTesting
      if (commitHook != null) commitHook()
      OpenIvmSnapshotArchive.checkInterrupted()
      if (!fs.rename(pendingCommit, committed))
        throw new java.io.IOException(s"openivm state-sync: cannot publish snapshot commit $committed")
      trace.markPublished()
    }
    onCommitted()
    log.info(
      s"openivm state-sync: published packed snapshot $generation ($archiveBytes bytes) at $uri"
    )
    val pruneStarted = trace.beginPhase("spark.backup.prune", "lifecycle")
    val pruned       = pruneGenerations(fs, controlRoot, FeatureGate.stateSyncRetainGenerations(spark))
    trace.markPruned(pruned.deletedPaths)
    trace.endPhase(
      pruneStarted,
      if (pruned.failure.isDefined) "failed" else "success",
      counts = Map("deleted_paths" -> pruned.deletedPaths),
      error = pruned.failure
    )
  }

  // Explicit guarded entry points; there is no configured OneLake transport or legacy fallback.
  private[rocksdb] def publishGuarded(
      spark: SparkSession,
      generation: GuardedSnapshotDecisions.Generation,
      store: GuardedSnapshotDecisions.ArchiveStore
  ): Unit =
    publishGuardedWithTrace(spark, generation, store, new BackupPassTrace(Seq.empty, "synchronous", ""))

  private def publishGuardedWithTrace(
      spark: SparkSession,
      generation: GuardedSnapshotDecisions.Generation,
      store: GuardedSnapshotDecisions.ArchiveStore,
      trace: BackupPassTrace
  ): Unit = {
    store.verifyNamespace()
    val local = localRoot(spark)
    withPackedSnapshot(spark, local, generation.value, trace, allowReuse = false) { (archive, length, digest, _, _) =>
      val receipt = trace.measure("spark.backup.upload", "durability") {
        val uploaded = store.sealUpload(generation, archive, length, digest)
        trace.markUploaded(uploaded.bytes)
        uploaded
      }
      if (receipt.generation != generation || receipt.bytes != length || receipt.sha256 != digest)
        throw new java.io.IOException("openivm state-sync: guarded upload receipt does not match local archive")
      val commitHook = beforeSnapshotCommitHookForTesting
      if (commitHook != null) commitHook()
      OpenIvmSnapshotArchive.checkInterrupted()
      trace.measure("spark.backup.commit", "durability") {
        new GuardedSnapshotDecisions(store).publish(receipt)
        trace.markPublished()
      }
    }
  }

  private[rocksdb] def restoreGuarded(
      spark: SparkSession,
      store: GuardedSnapshotDecisions.ArchiveStore
  ): Option[GuardedSnapshotDecisions.Generation] =
    OpenIvmStateBarrier.withCapture(spark) {
      val selected = new GuardedSnapshotDecisions(store).latestPublished()
      val local    = localRoot(spark)
      if (hasLocalStateFiles(local))
        throw new java.io.IOException("openivm state-sync: guarded restore requires an empty local state root")
      selected.map { receipt =>
        restorePackedArchive(receipt.bytes -> receipt.sha256, local)(archive =>
          store.copyExactArchive(receipt, archive)
        )
        receipt.generation
      }
    }

  /** Delete committed generations older than the newest `retain`, then snapshots and pending
    * commit markers older than every retained generation, which interrupted publishes left behind.
    * A generation is unpublished before its archive is deleted, so restore never selects it. At
    * most [[MaxPrunedPerPublish]] entries go per publish, so a large backlog never stalls one.
    */
  private def pruneGenerations(fs: FileSystem, controlRoot: Path, retain: Int): PruneResult = {
    var deletedPaths = 0L
    try {
      val commitRoot    = new Path(controlRoot, "commits")
      val snapshotsRoot = new Path(controlRoot, "snapshots")
      val committed = fs
        .listStatus(commitRoot)
        .toSeq
        .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
        .map(_.getPath.getName.stripSuffix(".commit"))
        .sorted
      val retained = committed.takeRight(retain)
      val expired  = committed.dropRight(retain).take(MaxPrunedPerPublish)
      expired.foreach { generation =>
        if (fs.delete(new Path(commitRoot, s"$generation.commit"), false)) deletedPaths += 1
        if (fs.delete(new Path(snapshotsRoot, generation), true)) deletedPaths += 1
      }
      val budget = MaxPrunedPerPublish - expired.size
      if (budget > 0) retained.headOption.foreach { oldestRetained =>
        val staleSnapshots = fs
          .listStatus(snapshotsRoot)
          .toSeq
          .filter(status => status.isDirectory && status.getPath.getName < oldestRetained)
          .map(_.getPath)
        val stalePending = fs
          .listStatus(commitRoot)
          .toSeq
          .filter { status =>
            val name = status.getPath.getName
            name.startsWith(".") && name.endsWith(".pending") && name.stripPrefix(".") < oldestRetained
          }
          .map(_.getPath)
        (staleSnapshots ++ stalePending).take(budget).foreach { path =>
          if (fs.delete(path, true)) deletedPaths += 1
        }
      }
      PruneResult(deletedPaths, None)
    } catch {
      case NonFatal(error) =>
        log.warn(s"openivm state-sync: cannot prune old snapshot generations: $error")
        PruneResult(deletedPaths, Some(error))
    }
  }

  private def backupFiles(local: File): Seq[File] =
    listLocalFiles(local)
      .filterNot(file => isTransientRocksDbFile(file.getName))
      .sortBy(_.getAbsolutePath)

  private def isTransientRocksDbFile(name: String): Boolean =
    name.endsWith(".crc") ||
      name == "LOCK" ||
      name == "LOG" ||
      name.startsWith("LOG.old.") ||
      name.startsWith("OPTIONS-") ||
      name.endsWith(".dbtmp")

  private[openivm] def setBackupPassHookForTesting(hook: (String, SparkSession, String) => Unit): Unit =
    backupPassHookForTesting = hook

  private[rocksdb] def setStateSyncKeyHookForTesting(hook: (SparkSession, String) => String): Unit =
    stateSyncKeyHookForTesting = hook

  private[openivm] def setStateSyncUriHookForTesting(hook: SparkSession => Option[String]): Unit =
    stateSyncUriHookForTesting = hook

  private[rocksdb] def setBeforeLocalFileCopyHookForTesting(hook: File => Unit): Unit =
    beforeLocalFileCopyHookForTesting = hook

  private[rocksdb] def setBeforeCaptureHookForTesting(hook: () => Unit): Unit =
    beforeCaptureHookForTesting = hook

  private[rocksdb] def setAfterCandidatePublicationHookForTesting(hook: () => Unit): Unit =
    afterCandidatePublicationHookForTesting = hook

  private[rocksdb] def setBeforeSnapshotCommitHookForTesting(hook: () => Unit): Unit =
    beforeSnapshotCommitHookForTesting = hook

  private[rocksdb] def setAfterSnapshotCommitHookForTesting(hook: () => Unit): Unit =
    afterSnapshotCommitHookForTesting = hook

  private[rocksdb] def setCopyLocalFileHookForTesting(hook: (FileSystem, File, Path) => Unit): Unit =
    copyLocalFileHookForTesting = hook

  private[rocksdb] def setStateSyncRequestReceiptHookForTesting(
      hook: StateSyncRequestReceipt => Unit
  ): Unit = stateSyncRequestReceiptHookForTesting = hook

  private final case class CommittedSnapshot(root: Path, archive: Option[(Long, String)])

  private def latestCommit(fs: FileSystem, namespaceRoot: Path): Option[CommittedSnapshot] = {
    val controlRoot = new Path(namespaceRoot, ".openivm-state-sync")
    val commitRoot  = new Path(controlRoot, "commits")
    if (!fs.exists(commitRoot)) return None
    fs.listStatus(commitRoot)
      .toSeq
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .sortBy(_.getPath.getName)
      .lastOption
      .map { commit =>
        if (commit.getLen > 4096) throw new java.io.IOException("openivm state-sync: oversized snapshot commit")
        val input = fs.open(commit.getPath)
        val record =
          try scala.io.Source.fromInputStream(input, "UTF-8").mkString
          finally input.close()
        val generation = commit.getPath.getName.stripSuffix(".commit")
        if (!generation.matches("[0-9]{19}-[0-9a-f-]{36}"))
          throw new java.io.IOException(s"openivm state-sync: invalid snapshot commit ${commit.getPath}")
        val archive =
          if (record == s"OPENIVM_STATE_SNAPSHOT_V1\n$generation\n") None
          else {
            val lines = record.split("\\n", -1)
            if (
              lines.length != 5 || lines(0) != "OPENIVM_STATE_SNAPSHOT_V2" ||
              lines(1) != generation || !lines(2).matches("[0-9]{1,12}") ||
              !lines(3).matches("[0-9a-f]{64}") || lines(4).nonEmpty
            )
              throw new java.io.IOException(s"openivm state-sync: invalid snapshot commit ${commit.getPath}")
            val length = lines(2).toLong
            if (length > OpenIvmSnapshotArchive.MaxBytes)
              throw new java.io.IOException("openivm state-sync: archive size limit exceeded")
            Some(length -> lines(3))
          }
        val snapshot = new Path(new Path(controlRoot, "snapshots"), generation)
        if (!fs.exists(snapshot))
          throw new java.io.IOException(s"openivm state-sync: missing committed snapshot $snapshot")
        CommittedSnapshot(snapshot, archive)
      }
  }

  private[rocksdb] def latestCommittedSnapshot(fs: FileSystem, root: Path): Option[Path] =
    latestCommit(fs, root).map(_.root)

  private def restoreArchive(fs: FileSystem, root: Path, info: (Long, String), local: File): Unit = {
    val remote = new Path(root, "snapshot.zip")
    if (fs.getFileStatus(remote).getLen != info._1)
      throw new java.io.IOException("openivm state-sync: archive length mismatch")
    restorePackedArchive(info, local)(archive =>
      fs.copyToLocalFile(false, remote, new Path(archive.getAbsolutePath), true)
    )
  }

  private def restorePackedArchive(info: (Long, String), local: File)(download: File => Unit): Unit = {
    val suffix  = UUID.randomUUID().toString
    val archive = new File(local.getParentFile, s".openivm-state-restore-$suffix.zip")
    val staged  = new File(local.getParentFile, s".openivm-state-restore-$suffix")
    Files.createDirectories(local.getParentFile.toPath)
    try {
      download(archive)
      if (archive.length() != info._1 || OpenIvmSnapshotArchive.sha256(archive) != info._2)
        throw new java.io.IOException("openivm state-sync: archive checksum mismatch")
      Files.createDirectories(staged.toPath)
      OpenIvmSnapshotArchive.extract(archive, staged)
      if (hasLocalStateFiles(local))
        throw new java.io.IOException("openivm state-sync: local state appeared during restore")
      removeEmptyDirectories(local)
      Files.move(staged.toPath, local.toPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
    } finally {
      Files.deleteIfExists(archive.toPath)
      deleteStagedTree(staged)
    }
  }

  private def removeEmptyDirectories(file: File): Unit =
    if (file.isDirectory) {
      Option(file.listFiles()).toSeq.flatten.foreach(removeEmptyDirectories)
      Files.delete(file.toPath)
    }

  private def deleteStagedTree(file: File): Unit =
    if (file.exists()) {
      if (file.isDirectory) Option(file.listFiles()).toSeq.flatten.foreach(deleteStagedTree)
      Files.deleteIfExists(file.toPath)
    }

  private[rocksdb] def queuedPassCountForTesting(spark: SparkSession): Int =
    stateSyncUri(spark)
      .flatMap(uri => Option(backupStates.get(effectiveStateSyncKey(spark, uri))))
      .map(_.passLock.getQueueLength)
      .getOrElse(0)

  private[rocksdb] def destinationQueueLengthForTesting(spark: SparkSession): Int =
    stateSyncUri(spark).map(uri => destinationLock(uri).getQueueLength).getOrElse(0)

  private[rocksdb] def synchronousRequestCountForTesting(spark: SparkSession): Long =
    stateSyncUri(spark)
      .flatMap(uri => Option(backupStates.get(effectiveStateSyncKey(spark, uri))))
      .map(_.synchronousRequestCount)
      .getOrElse(0L)

  private[rocksdb] def backupStateSnapshotForTesting(spark: SparkSession): Option[BackupStateSnapshot] =
    stateSyncUri(spark)
      .flatMap(uri => Option(backupStates.get(effectiveStateSyncKey(spark, uri))))
      .map(state => BackupStateSnapshot(running = state.running.get(), requested = state.requested.get()))

  private[rocksdb] def allBackupStatesIdleForTesting: Boolean = {
    val it   = backupStates.values().iterator()
    var idle = true
    while (idle && it.hasNext) {
      val state = it.next()
      idle = !state.running.get() && !state.requested.get()
    }
    idle
  }

  private[rocksdb] def resetForTesting(): Unit = {
    backupPassHookForTesting = null
    stateSyncKeyHookForTesting = null
    stateSyncUriHookForTesting = null
    beforeLocalFileCopyHookForTesting = null
    beforeCaptureHookForTesting = null
    afterCandidatePublicationHookForTesting = null
    copyLocalFileHookForTesting = null
    beforeSnapshotCommitHookForTesting = null
    afterSnapshotCommitHookForTesting = null
    OpenIvmExecutionSpan.resetStateSyncForTesting()
    stateSyncRequestReceiptHookForTesting = null
    restoreStates.clear()
    backupStates.clear()
    destinationLocks.clear()
    stateSyncRootsByUri.clear()
    sharedStateSyncUris.clear()
  }

  private def relativize(root: Path, child: Path): String = {
    val r = root.toUri.getPath.stripSuffix("/")
    val c = child.toUri.getPath
    if (c.startsWith(r + "/")) c.substring(r.length + 1) else child.getName
  }

  private def listLocalFiles(dir: File): Seq[File] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[File]
    def walk(f: File): Unit =
      if (Files.isSymbolicLink(f.toPath))
        throw new java.io.IOException("openivm state-sync: symbolic state path")
      else if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(walk))
      else out += f
    walk(dir)
    out.toSeq
  }
}
