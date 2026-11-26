package org.openivm.spark.telemetry

import org.apache.spark.{ExecutorLostFailure, Resubmitted, Success, TaskKilled}
import org.apache.spark.scheduler.SparkListenerTaskEnd

import scala.collection.mutable

private[telemetry] object StageTaskSummary {
  val SampleLimit = 8
}

/** Counts physical attempts, including failed and speculative work; never replicated per referencing job. */
private[telemetry] final class StageTaskSummary {
  private val totals = mutable.ArrayBuffer.empty[Total]
  private final class Total(val name: String) {
    totals += this
    var sum   = 0L
    var count = 0L
    def +=(value: Long): Unit =
      if (value >= 0L) { sum = Math.addExact(sum, value); count += 1 }
  }
  private val runMs               = new Total("executor_run_ms")
  private val cpuNs               = new Total("executor_cpu_ns")
  private val deserializeMs       = new Total("executor_deserialize_ms")
  private val deserializeCpuNs    = new Total("executor_deserialize_cpu_ns")
  private val gcMs                = new Total("jvm_gc_ms")
  private val serializeMs         = new Total("result_serialization_ms")
  private val memorySpill         = new Total("memory_spill_bytes")
  private val diskSpill           = new Total("disk_spill_bytes")
  private val inputRecords        = new Total("input_records")
  private val inputBytes          = new Total("input_bytes")
  private val outputRecords       = new Total("output_records")
  private val outputBytes         = new Total("output_bytes")
  private val shuffleReadRecords  = new Total("shuffle_read_records")
  private val shuffleReadBytes    = new Total("shuffle_read_bytes")
  private val shuffleRemoteBytes  = new Total("shuffle_remote_bytes")
  private val fetchWaitMs         = new Total("shuffle_fetch_wait_ms")
  private val shuffleWriteRecords = new Total("shuffle_write_records")
  private val shuffleWriteBytes   = new Total("shuffle_write_bytes")
  private val shuffleWriteNs      = new Total("shuffle_write_ns")

  private val sample      = mutable.ArrayBuffer.empty[Map[String, Any]]
  private var attempts    = 0L
  private var withInfo    = 0L
  private var withMetrics = 0L
  private var failed      = 0L
  private var killed      = 0L
  private var lost        = 0L
  private var unknown     = 0L
  private var resubmitted = 0L
  private var nonfirst    = 0L
  private var speculative = 0L
  private var firstLaunch = Long.MaxValue
  private var longest     = -1L
  private var durations   = 0L
  private var durationSum = 0L
  var valid: Boolean      = true

  def observe(event: SparkListenerTaskEnd): Unit = {
    // Spark may notify the driver that a previously completed shuffle output must be recomputed.
    if (event.reason == Resubmitted) { resubmitted += 1; return }
    attempts += 1
    val outcome = event.reason match {
      case Success                => "success"
      case _: ExecutorLostFailure => lost += 1; failed += 1; "executor_lost"
      case _: TaskKilled          => killed += 1; "killed"
      case null                   => unknown += 1; "unknown"
      case _                      => failed += 1; "failed"
    }

    val info = event.taskInfo
    if (info != null) {
      withInfo += 1
      if (info.attemptNumber > 0) nonfirst += 1
      if (info.speculative) speculative += 1
      val launch = info.launchTime
      val finish = info.finishTime
      if (launch > 0L) firstLaunch = math.min(firstLaunch, launch)
      val timed = launch > 0L && finish >= launch
      if (timed) {
        durations += 1
        durationSum = Math.addExact(durationSum, finish - launch)
        longest = math.max(longest, finish - launch)
      }
      if (sample.size < StageTaskSummary.SampleLimit) {
        val identity = Map[String, Any](
          "task_id"     -> info.taskId,
          "index"       -> info.index,
          "attempt"     -> info.attemptNumber,
          "speculative" -> info.speculative,
          "outcome"     -> outcome
        )
        val executor = Option(info.executorId).filter(s =>
          s.nonEmpty && s.length <= 64 &&
            s.forall(c => c.isLetterOrDigit || "._-".contains(c))
        )
        sample += (identity ++ executor.map("executor_id" -> _) ++
          (if (timed) Some("duration_ms" -> (finish - launch)) else None))
      }
    }

    val m = event.taskMetrics
    if (m != null) {
      withMetrics += 1
      runMs += m.executorRunTime
      cpuNs += m.executorCpuTime
      deserializeMs += m.executorDeserializeTime
      deserializeCpuNs += m.executorDeserializeCpuTime
      gcMs += m.jvmGCTime
      serializeMs += m.resultSerializationTime
      memorySpill += m.memoryBytesSpilled
      diskSpill += m.diskBytesSpilled
      val in = m.inputMetrics
      if (in != null) { inputRecords += in.recordsRead; inputBytes += in.bytesRead }
      val out = m.outputMetrics
      if (out != null) { outputRecords += out.recordsWritten; outputBytes += out.bytesWritten }
      val read = m.shuffleReadMetrics
      if (read != null) {
        shuffleReadRecords += read.recordsRead
        shuffleReadBytes += read.totalBytesRead
        shuffleRemoteBytes += read.remoteBytesRead
        fetchWaitMs += read.fetchWaitTime
      }
      val write = m.shuffleWriteMetrics
      if (write != null) {
        shuffleWriteRecords += write.recordsWritten
        shuffleWriteBytes += write.bytesWritten
        shuffleWriteNs += write.writeTime
      }
    }
  }

  def fields(submitted: Option[Long]): Map[String, Any] = {
    val counters = Map[String, Any](
      "attempts"                  -> attempts,
      "attempts_with_info"        -> withInfo,
      "attempts_with_metrics"     -> withMetrics,
      "attempts_without_metrics"  -> (attempts - withMetrics),
      "failed_attempts"           -> failed,
      "killed_attempts"           -> killed,
      "executor_lost_attempts"    -> lost,
      "unknown_outcomes"          -> unknown,
      "resubmitted_notifications" -> resubmitted,
      "duration_count"            -> durations,
      "sample"                    -> sample.toVector,
      "sample_truncated"          -> (withInfo > sample.size),
      "sample_dropped_count"      -> (withInfo - sample.size)
    )
    val partial = totals.iterator.collect { case t if t.count > 0L && t.count < attempts => t.name -> t.count }.toMap
    counters ++ totals.iterator.collect {
      case t if t.count > 0L => t.name -> t.sum
    } ++
      (if (partial.nonEmpty) Some("partial_metric_counts" -> partial) else None) ++
      (if (withInfo > 0L) Map("nonfirst_attempts" -> nonfirst, "speculative_attempts" -> speculative) else Map.empty) ++
      (if (durations > 0L) Map("duration_sum_ms" -> durationSum, "duration_max_ms" -> longest) else Map.empty) ++
      (if (firstLaunch != Long.MaxValue) Map("first_task_launch_epoch_ms" -> firstLaunch) else Map.empty) ++
      submitted
        .filter(t => t >= 0L && firstLaunch != Long.MaxValue && t <= firstLaunch)
        .map(t => "submit_to_first_observed_launch_ms" -> (firstLaunch - t))
  }
}
