package org.openivm.spark.telemetry

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{ExecutorLostFailure, Resubmitted, SparkConf, SparkContext, Success, TaskEndReason, TaskKilled}
import org.apache.spark.scheduler.{
  SparkListenerJobStart,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd,
  StageInfo,
  TaskInfo
}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class TelemetryJobListenerSpec extends AnyFunSpec with Matchers {
  private def stage(id: Int, attempt: Int = 0): StageInfo =
    new StageInfo(id, attempt, "fixture", 1, Nil, Nil, "", resourceProfileId = 0)

  private def task(id: Long, stageId: Int, attempt: Int, reason: TaskEndReason): SparkListenerTaskEnd = {
    val info = new TaskInfo(id, 0, attempt, 0, 100L, "1", "localhost", null, attempt > 1)
    info.finishTime = 110L + id
    SparkListenerTaskEnd(stageId, 0, "ResultTask", reason, info, null, null)
  }

  private def gauge(registry: MetricRegistry, name: String): Long =
    registry.getGauges.get(s"openivm.telemetry.listener.$name").getValue.asInstanceOf[Number].longValue()

  it("correlates shared stages to jobs and stops reporting after their last owner ends") {
    val lines    = ArrayBuffer.empty[String]
    val listener = new TelemetryJobListener(line => { lines += line; () })
    listener.recordJobStart(1, 100L, Seq(4, 5, 5), "refresh-1/action-1", Some("sql-1"))
    listener.recordJobStart(2, 110L, Seq(5), "refresh-2/action-1", None)
    listener.recordJobEnd(1, 120L, "success")
    listener.recordStageCompleted(4, 0, Some(101L), Some(120L), successful = true)
    listener.recordStageCompleted(5, 1, Some(102L), Some(130L), successful = false)
    listener.recordJobEnd(2, 140L, "failed")
    listener.recordStageCompleted(5, 2, Some(131L), Some(141L), successful = true)

    val mapper = new ObjectMapper()
    val events = lines.map(line => mapper.readTree(line))
    val first  = events.head
    first.get("action_context").asText() shouldBe "refresh-1/action-1"
    first.get("spark_sql_execution_id").asText() shouldBe "sql-1"
    first.get("stage_ids").size() shouldBe 2
    val stages = events.filter(_.get("event_type").asText() == "spark_stage_completed")
    stages.size shouldBe 1
    stages.head.get("stage_id").asInt() shouldBe 5
    stages.head.get("stage_attempt").asInt() shouldBe 1
    stages.head.get("outcome").asText() shouldBe "failed"
  }

  it("marks correlations truncated when a job exceeds the bounded stage limit") {
    val lines    = ArrayBuffer.empty[String]
    val listener = new TelemetryJobListener(line => { lines += line; () })
    listener.recordJobStart(1, 0L, 0 until 300, "refresh/action", None)
    val event = new ObjectMapper().readTree(lines.head)
    event.get("stage_tracking_truncated").asBoolean() shouldBe true
    event.get("stage_ids").size() shouldBe 256
  }

  it("counts failed, retried and speculative attempts once for a shared stage without fabricating missing metrics") {
    val lines    = ArrayBuffer.empty[String]
    val listener = new TelemetryJobListener(line => { lines += line; () }, taskMetricsEnabled = true)
    val shared   = stage(9)
    listener.recordJobStart(1, 10L, Seq(9), "a", None)
    listener.recordJobStart(2, 20L, Seq(9), "b", None)
    listener.onStageSubmitted(SparkListenerStageSubmitted(shared))
    listener.onTaskEnd(task(0L, 9, 0, ExecutorLostFailure("1", false, Some("sensitive details"))))
    listener.recordJobEnd(1, 30L, "failed")
    listener.onTaskEnd(task(1L, 9, 1, Success))
    listener.onTaskEnd(task(1L, 9, 1, Resubmitted))
    listener.onTaskEnd(task(2L, 9, 2, TaskKilled("sensitive details")))
    (3L until 11L).foreach(id => listener.onTaskEnd(task(id, 9, 0, Success)))
    listener.recordStageCompleted(9, 0, Some(99L), Some(150L), successful = true)
    listener.recordJobEnd(2, 160L, "success")
    listener.recordJobStart(3, 170L, Seq(9), "reused", None)
    listener.recordJobEnd(3, 180L, "success")
    val records = lines.filter(_.contains("spark_stage_completed"))
    records.size shouldBe 1
    records.head should not include "sensitive"
    val summary = new ObjectMapper().readTree(records.head).get("task_summary")
    summary.get("attempts").asLong() shouldBe 11L
    summary.get("failed_attempts").asLong() shouldBe 1L
    summary.get("executor_lost_attempts").asLong() shouldBe 1L
    summary.get("killed_attempts").asLong() shouldBe 1L
    summary.get("nonfirst_attempts").asLong() shouldBe 2L
    summary.get("speculative_attempts").asLong() shouldBe 1L
    summary.get("attempts_with_metrics").asLong() shouldBe 0L
    summary.has("executor_run_ms") shouldBe false
    summary.get("duration_sum_ms").asLong() shouldBe 165L
    summary.get("resubmitted_notifications").asLong() shouldBe 1L
    summary.get("submit_to_first_observed_launch_ms").asLong() shouldBe 1L
    summary.get("duration_max_ms").asLong() shouldBe 20L
    summary.get("sample").size() shouldBe 8
    summary.get("sample").get(2).get("outcome").asText() shouldBe "killed"
    summary.get("sample_dropped_count").asLong() shouldBe 3L
    summary.get("sample_truncated").asBoolean() shouldBe true
  }

  it("reports capacity loss, releases abandoned state and isolates emitter failures") {
    val registry = new MetricRegistry()
    val lines    = ArrayBuffer.empty[String]
    var fail     = false
    val listener = new TelemetryJobListener(
      line => {
        if (fail) throw new IllegalStateException("test sink")
        lines += line; ()
      },
      taskMetricsEnabled = true,
      maxActiveAttempts = 1
    )
    listener.registerMetrics(registry)
    val a = stage(10)
    val b = stage(11)
    listener.recordJobStart(1, 0L, Seq(10), "a", None)
    listener.recordJobStart(2, 0L, Seq(11), "b", None)
    listener.onStageSubmitted(SparkListenerStageSubmitted(a))
    listener.onStageSubmitted(SparkListenerStageSubmitted(b))
    listener.onTaskEnd(task(1L, 11, 0, Success))
    listener.recordJobEnd(1, 10L, "failed")
    listener.recordStageCompleted(11, 0, None, None, successful = false)
    val skipped = new ObjectMapper().readTree(lines.last)
    skipped.get("task_summary_status").asText() shouldBe "submission_not_tracked"
    skipped.has("task_summary") shouldBe false
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage(11, 1)))
    listener.onTaskEnd(task(2L, 11, 0, Success))
    listener.recordStageCompleted(11, 1, None, None, successful = false)
    val resumed = new ObjectMapper().readTree(lines.last).get("task_summary")
    resumed.get("attempts").asLong() shouldBe 0L
    resumed.has("duration_sum_ms") shouldBe false
    resumed.has("input_bytes") shouldBe false
    gauge(registry, "abandoned_stage_attempts") shouldBe 1L
    gauge(registry, "stage_attempt_capacity_drops") shouldBe 1L
    gauge(registry, "untracked_task_ends") shouldBe 2L
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage(11, 2)))
    (1L to 2L).foreach { id =>
      val e = task(id, 11, 0, Success).copy(stageAttemptId = 2)
      e.taskInfo.finishTime = Long.MaxValue
      listener.onTaskEnd(e)
    }
    listener.recordStageCompleted(11, 2, None, None, successful = true)
    val invalid = new ObjectMapper().readTree(lines.last)
    invalid.get("task_summary_status").asText() shouldBe "callback_error"
    invalid.has("task_summary") shouldBe false
    val props = new Properties()
    props.setProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, "op-2")
    fail = true
    listener.onJobStart(SparkListenerJobStart(20, 20L, Seq(stage(12)), props))
    fail = false
    gauge(registry, "callback_errors") shouldBe 2L
    gauge(registry, "observed_callback_ns") should be > 0L
    listener.recordJobEnd(20, 30L, "success")
    new ObjectMapper().readTree(lines.last).get("job_id").asInt() shouldBe 20
  }

  it("keeps request-only work and listener gauges off when deep metrics are disabled") {
    val lines    = ArrayBuffer.empty[String]
    val registry = new MetricRegistry()
    val listener = new TelemetryJobListener(line => { lines += line; () })
    listener.registerMetrics(registry)
    val props = new Properties()
    props.setProperty(OpenIvmTelemetryContract.RequestIdProperty, "request-1")
    listener.onJobStart(SparkListenerJobStart(1, 1L, Seq(stage(4)), props))
    lines shouldBe empty
    registry.getNames shouldBe empty
    props.setProperty(TelemetryJobListener.ActionProperty, "action-1")
    listener.onJobStart(SparkListenerJobStart(2, 2L, Seq(stage(4)), props))
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage(4)))
    listener.onTaskEnd(task(1L, 4, 0, Success))
    listener.recordStageCompleted(4, 0, None, None, successful = true)
    val mapper = new ObjectMapper()
    mapper.readTree(lines.head).has("request_id") shouldBe false
    mapper.readTree(lines.last).has("task_summary_status") shouldBe false
    mapper.readTree(lines.last).has("task_summary") shouldBe false
  }

  it("exports real Spark job and stage events through the driver plugin") {
    val root = Files.createTempDirectory("openivm-telemetry-job")
    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("openivm-telemetry-job")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.plugins", "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin")
      .set(BoundedTelemetryQueue.EnabledKey, "true")
      .set(TelemetryJobListener.TaskMetricsKey, "true")
      .set(BoundedTelemetryQueue.LocalUriKey, root.toUri.toString)
    val sc = new SparkContext(conf)
    try {
      sc.setLocalProperty(TelemetryJobListener.ActionProperty, "refresh-1/action-1")
      sc.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, "op-1")
      sc.setLocalProperty(OpenIvmTelemetryContract.RequestIdProperty, "request-1")
      sc.parallelize(Seq(1 -> 1, 1 -> 1), 2).reduceByKey(_ + _, 1).collect().toSeq shouldBe Seq(1 -> 2)
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
      while (BoundedTelemetryRuntime.health.forall(_.published < 4L) && System.nanoTime() < deadline)
        Thread.sleep(25L)
      BoundedTelemetryRuntime.health.get.published should be >= 4L
    } finally sc.stop()
    val stream = Files.list(root)
    val paths =
      try stream.iterator().asScala.filter(_.toString.endsWith(".jsonl")).toVector
      finally stream.close()
    val mapper = new ObjectMapper()
    val events = paths.flatMap(path => Files.readAllLines(path, UTF_8).asScala).map(line => mapper.readTree(line))
    val start  = events.find(_.get("event_type").asText() == "spark_job_start").get
    start.get("action_context").asText() shouldBe "refresh-1/action-1"
    start.get("operation_id").asText() shouldBe "op-1"
    start.get("request_id").asText() shouldBe "request-1"
    val summaries = events.filter(_.get("event_type").asText() == "spark_stage_completed").map(_.get("task_summary"))
    summaries.map(_.get("attempts_with_metrics").asLong()).sum shouldBe 3L
    summaries.map(_.get("shuffle_write_records").asLong()).sum shouldBe 2L
    summaries.map(_.get("shuffle_read_records").asLong()).sum shouldBe 2L
    summaries.map(_.get("shuffle_read_bytes").asLong()).sum should be > 0L
    summaries.foreach(s => s.has("executor_cpu_ns") shouldBe true)
    val stage = events.find(_.get("event_type").asText() == "spark_stage_completed").get
    start.get("stage_ids").elements().asScala.map(_.asInt()).toSeq should contain(stage.get("stage_id").asInt())
    events.exists(e =>
      e.get("event_type").asText() == "spark_job_end" &&
        e.get("job_id").asInt() == start.get("job_id").asInt()
    ) shouldBe true
  }
}
