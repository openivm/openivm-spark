package org.openivm.spark.telemetry

import java.time.Instant

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}
import org.openivm.spark.common.MonotonicClockDomain
import org.openivm.spark.insights.OpenIvmInsightJson

import scala.collection.mutable

private[spark] object TelemetryJobListener {
  val ActionProperty = "openivm.telemetry.action"
  private val MaxJobs = 1024
  private val MaxStagesPerJob = 256
  private val MaxStageReferences = 8192
}

private[spark] final class TelemetryJobListener(emit: String => Unit = BoundedTelemetryRuntime.offer)
    extends SparkListener {
  import TelemetryJobListener._

  private val jobs = mutable.Map.empty[Int, Vector[Int]]
  private val stageReferences = mutable.Map.empty[Int, Int]
  private var referenceCount = 0

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val properties = Option(event.properties)
    val context = properties.flatMap(p => Option(p.getProperty(ActionProperty)))
    context.foreach { action =>
      recordJobStart(
        event.jobId,
        event.time,
        event.stageInfos.iterator.map(_.stageId).toVector,
        action,
        properties.flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
      )
    }
  }

  private[telemetry] def recordJobStart(
      jobId: Int,
      startedEpochMs: Long,
      stageIds: Seq[Int],
      action: String,
      sqlExecutionId: Option[String]
  ): Unit = {
      val stages = stageIds.toVector.distinct
      val admitted = if (jobs.size >= MaxJobs) Vector.empty[Int]
      else stages.take(math.min(MaxStagesPerJob, MaxStageReferences - referenceCount))
      if (admitted.nonEmpty) {
        jobs.put(jobId, admitted)
        admitted.foreach { stage =>
          stageReferences.update(stage, stageReferences.getOrElse(stage, 0) + 1)
        }
        referenceCount += admitted.size
      }
      emit(OpenIvmInsightJson.obj(
        "event_type" -> "spark_job_start",
        "clock_domain_id" -> MonotonicClockDomain.id,
        "job_id" -> jobId,
        "started_at" -> Instant.ofEpochMilli(startedEpochMs).toString,
        "started_epoch_ms" -> startedEpochMs,
        "action_context" -> action,
        "spark_sql_execution_id" -> sqlExecutionId,
        "stage_ids" -> admitted,
        "stage_tracking_truncated" -> (admitted.size != stages.size)
      ))
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit =
    recordJobEnd(event.jobId, event.time, event.jobResult.getClass.getSimpleName)

  private[telemetry] def recordJobEnd(jobId: Int, completedEpochMs: Long, outcome: String): Unit = {
    jobs.remove(jobId).foreach { stages =>
      stages.foreach { stage =>
        val remaining = stageReferences(stage) - 1
        if (remaining == 0) stageReferences.remove(stage)
        else stageReferences.update(stage, remaining)
      }
      referenceCount -= stages.size
      emit(OpenIvmInsightJson.obj(
        "event_type" -> "spark_job_end",
        "clock_domain_id" -> MonotonicClockDomain.id,
        "job_id" -> jobId,
        "completed_at" -> Instant.ofEpochMilli(completedEpochMs).toString,
        "completed_epoch_ms" -> completedEpochMs,
        "outcome" -> outcome
      ))
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    if (info != null)
      recordStageCompleted(info.stageId, info.attemptNumber(), info.submissionTime, info.completionTime,
        info.failureReason.isEmpty)
  }

  private[telemetry] def recordStageCompleted(
      stageId: Int,
      attempt: Int,
      submitted: Option[Long],
      completed: Option[Long],
      successful: Boolean
  ): Unit = {
    if (stageReferences.contains(stageId)) {
      emit(OpenIvmInsightJson.obj(
        "event_type" -> "spark_stage_completed",
        "clock_domain_id" -> MonotonicClockDomain.id,
        "stage_id" -> stageId,
        "stage_attempt" -> attempt,
        "submitted_at" -> submitted.map(value => Instant.ofEpochMilli(value).toString),
        "completed_at" -> completed.map(value => Instant.ofEpochMilli(value).toString),
        "submitted_epoch_ms" -> submitted,
        "completed_epoch_ms" -> completed,
        "outcome" -> (if (successful) "success" else "failed")
      ))
    }
  }
}
