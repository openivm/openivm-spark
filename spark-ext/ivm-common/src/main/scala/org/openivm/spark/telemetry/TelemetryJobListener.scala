package org.openivm.spark.telemetry

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerExecutorRemoved,
  SparkListenerJobEnd,
  SparkListenerJobStart,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}
import org.openivm.spark.common.MonotonicClockDomain
import org.openivm.spark.insights.OpenIvmInsightJson

import scala.collection.mutable
import scala.util.control.NonFatal

private[spark] object TelemetryJobListener {
  val ActionProperty             = "openivm.telemetry.action"
  val TaskMetricsKey             = "spark.openivm.telemetry.taskMetrics.enabled"
  private val MaxJobs            = 1024
  private val MaxStagesPerJob    = 256
  private val MaxStageReferences = 8192
}

private[spark] final class TelemetryJobListener(
    emit: String => Unit = BoundedTelemetryRuntime.offer,
    taskMetricsEnabled: Boolean = false,
    maxActiveAttempts: Int = 512
) extends SparkListener {
  import TelemetryJobListener._

  private val jobs            = mutable.Map.empty[Int, Vector[Int]]
  private val stageReferences = mutable.Map.empty[Int, Int]
  private var referenceCount  = 0

  private val summaries          = mutable.Map.empty[Int, mutable.Map[Int, StageTaskSummary]]
  private var activeAttempts     = 0
  private val callbacks          = new AtomicLong()
  private val callbackNs         = new AtomicLong()
  private val callbackErrors     = new AtomicLong()
  private val capacityDrops      = new AtomicLong()
  private val untrackedTaskEnds  = new AtomicLong()
  private val abandonedAttempts  = new AtomicLong()
  private val truncatedJobs      = new AtomicLong()
  private val rejectedIdentities = new AtomicLong()
  private val executorRemovals   = new AtomicLong()

  private[spark] def registerMetrics(registry: MetricRegistry): Unit =
    if (taskMetricsEnabled) {
      Seq(
        "observed_callbacks"            -> callbacks,
        "observed_callback_ns"          -> callbackNs,
        "callback_errors"               -> callbackErrors,
        "stage_attempt_capacity_drops"  -> capacityDrops,
        "untracked_task_ends"           -> untrackedTaskEnds,
        "abandoned_stage_attempts"      -> abandonedAttempts,
        "truncated_jobs"                -> truncatedJobs,
        "application_executor_removals" -> executorRemovals,
        "rejected_identity_values"      -> rejectedIdentities
      ).foreach { case (name, value) =>
        registry.register(s"openivm.telemetry.listener.$name", new Gauge[Long] { def getValue: Long = value.get() })
      }
    }

  // Event state is confined to the Spark listener bus; registry gauges may be sampled on another thread.
  private def observed(body: => Unit): Unit =
    if (!taskMetricsEnabled) body
    else {
      val start = System.nanoTime()
      try body
      catch { case NonFatal(_) => callbackErrors.incrementAndGet(); () }
      finally {
        callbackNs.addAndGet(System.nanoTime() - start)
        callbacks.incrementAndGet()
      }
    }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val p = event.properties
    if (p == null) return
    val action = p.getProperty(ActionProperty)
    def id(key: String): Option[String] = Option(p.getProperty(key)).filter { s =>
      val valid = s.nonEmpty && s.length <= 512 && s.forall(c => c.isLetterOrDigit || "._:@-".contains(c))
      if (!valid) rejectedIdentities.incrementAndGet()
      valid
    }
    val operation = if (taskMetricsEnabled) id(OpenIvmExecutionSpan.TraceOperationIdProperty) else None
    val request   = if (taskMetricsEnabled) id(OpenIvmTelemetryContract.RequestIdProperty) else None
    if (action != null || operation.isDefined || request.isDefined) observed {
      recordStart(
        event.jobId,
        event.time,
        event.stageInfos.iterator.map(_.stageId),
        Option(action),
        Option(p.getProperty("spark.sql.execution.id")),
        request,
        operation
      )
    }
  }

  private[telemetry] def recordJobStart(
      jobId: Int,
      startedEpochMs: Long,
      stageIds: Seq[Int],
      action: String,
      sqlExecutionId: Option[String]
  ): Unit = recordStart(jobId, startedEpochMs, stageIds.iterator, Option(action), sqlExecutionId, None, None)

  private def recordStart(
      jobId: Int,
      startedEpochMs: Long,
      stageIds: Iterator[Int],
      action: Option[String],
      sqlExecutionId: Option[String],
      request: Option[String],
      operation: Option[String]
  ): Unit = {
    val admittedJob = jobs.size < MaxJobs && !jobs.contains(jobId)
    val limit       = if (admittedJob) math.min(MaxStagesPerJob, MaxStageReferences - referenceCount) else 0
    val stages      = mutable.LinkedHashSet.empty[Int]
    var truncated   = !admittedJob
    while (!truncated && stageIds.hasNext) {
      val id = stageIds.next()
      if (stages.size < limit) stages += id
      else if (!stages.contains(id)) truncated = true
    }
    val admitted = stages.toVector
    if (taskMetricsEnabled && truncated) truncatedJobs.incrementAndGet()
    if (admittedJob) {
      jobs.put(jobId, admitted)
      admitted.foreach { stage =>
        stageReferences.update(stage, stageReferences.getOrElse(stage, 0) + 1)
      }
      referenceCount += admitted.size
    }
    val fields = Seq[(String, Any)](
      "event_type"               -> "spark_job_start",
      "clock_domain_id"          -> MonotonicClockDomain.id,
      "job_id"                   -> jobId,
      "started_at"               -> Instant.ofEpochMilli(startedEpochMs).toString,
      "started_epoch_ms"         -> startedEpochMs,
      "action_context"           -> action,
      "spark_sql_execution_id"   -> sqlExecutionId,
      "stage_ids"                -> admitted,
      "stage_tracking_truncated" -> truncated
    ) ++ request.map("request_id" -> _) ++ operation.map("operation_id" -> _)
    emit(OpenIvmInsightJson.obj(fields: _*))
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit =
    if (jobs.contains(event.jobId)) observed {
      recordJobEnd(event.jobId, event.time, event.jobResult.getClass.getSimpleName)
    }

  private[telemetry] def recordJobEnd(jobId: Int, completedEpochMs: Long, outcome: String): Unit = {
    jobs.remove(jobId).foreach { stages =>
      stages.foreach { stage =>
        val remaining = stageReferences(stage) - 1
        if (remaining == 0) {
          stageReferences.remove(stage)
          if (taskMetricsEnabled) summaries.remove(stage).foreach { discarded =>
            activeAttempts -= discarded.size
            abandonedAttempts.addAndGet(discarded.size.toLong)
          }
        } else stageReferences.update(stage, remaining)
      }
      referenceCount -= stages.size
      emit(
        OpenIvmInsightJson.obj(
          "event_type"         -> "spark_job_end",
          "clock_domain_id"    -> MonotonicClockDomain.id,
          "job_id"             -> jobId,
          "completed_at"       -> Instant.ofEpochMilli(completedEpochMs).toString,
          "completed_epoch_ms" -> completedEpochMs,
          "outcome"            -> outcome
        )
      )
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit =
    if (taskMetricsEnabled && event.stageInfo != null && stageReferences.contains(event.stageInfo.stageId)) observed {
      val stage   = event.stageInfo
      val attempt = stage.attemptNumber()
      if (!summaries.get(stage.stageId).exists(_.contains(attempt))) {
        if (activeAttempts >= maxActiveAttempts) capacityDrops.incrementAndGet()
        else {
          summaries.getOrElseUpdate(stage.stageId, mutable.Map.empty).put(attempt, new StageTaskSummary)
          activeAttempts += 1
        }
      }
    }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    if (taskMetricsEnabled && stageReferences.contains(event.stageId)) observed {
      summaries.get(event.stageId).flatMap(_.get(event.stageAttemptId)) match {
        case Some(summary) if summary.valid =>
          try summary.observe(event)
          catch { case NonFatal(e) => summary.valid = false; throw e }
        case _ => untrackedTaskEnds.incrementAndGet()
      }
    }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit =
    if (taskMetricsEnabled) { executorRemovals.incrementAndGet(); () }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    if (info != null && stageReferences.contains(info.stageId)) observed {
      recordStageCompleted(
        info.stageId,
        info.attemptNumber(),
        info.submissionTime,
        info.completionTime,
        info.failureReason.isEmpty
      )
    }
  }

  private[telemetry] def recordStageCompleted(
      stageId: Int,
      attempt: Int,
      submitted: Option[Long],
      completed: Option[Long],
      successful: Boolean
  ): Unit = {
    if (stageReferences.contains(stageId)) {
      val taskFields =
        if (!taskMetricsEnabled) Seq.empty
        else {
          val summary = summaries.get(stageId).flatMap { attempts =>
            val result = attempts.remove(attempt)
            if (result.isDefined) activeAttempts -= 1
            if (attempts.isEmpty) summaries.remove(stageId)
            result
          }
          summary match {
            case Some(value) if value.valid =>
              Seq("task_summary_status" -> "observed", "task_summary" -> value.fields(submitted))
            case Some(_) => Seq("task_summary_status" -> "callback_error")
            case None    => Seq("task_summary_status" -> "submission_not_tracked")
          }
        }
      val fields = Seq[(String, Any)](
        "event_type"         -> "spark_stage_completed",
        "clock_domain_id"    -> MonotonicClockDomain.id,
        "stage_id"           -> stageId,
        "stage_attempt"      -> attempt,
        "submitted_at"       -> submitted.map(value => Instant.ofEpochMilli(value).toString),
        "completed_at"       -> completed.map(value => Instant.ofEpochMilli(value).toString),
        "submitted_epoch_ms" -> submitted,
        "completed_epoch_ms" -> completed,
        "outcome"            -> (if (successful) "success" else "failed")
      ) ++ taskFields
      emit(OpenIvmInsightJson.obj(fields: _*))
    }
  }
}
