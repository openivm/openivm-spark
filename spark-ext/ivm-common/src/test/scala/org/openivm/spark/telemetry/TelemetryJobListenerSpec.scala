package org.openivm.spark.telemetry

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class TelemetryJobListenerSpec extends AnyFunSpec with Matchers {
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
      .set(BoundedTelemetryQueue.LocalUriKey, root.toUri.toString)
    val sc = new SparkContext(conf)
    try {
      sc.setLocalProperty(TelemetryJobListener.ActionProperty, "refresh-1/action-1")
      sc.parallelize(Seq(1, 2), 1).count() shouldBe 2L
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
      while (BoundedTelemetryRuntime.health.forall(_.published < 3L) && System.nanoTime() < deadline)
        Thread.sleep(25L)
      BoundedTelemetryRuntime.health.get.published should be >= 3L
    } finally sc.stop()
    val stream = Files.list(root)
    val paths =
      try stream.iterator().asScala.filter(_.toString.endsWith(".jsonl")).toVector
      finally stream.close()
    val mapper = new ObjectMapper()
    val events = paths.flatMap(path => Files.readAllLines(path, UTF_8).asScala).map(line => mapper.readTree(line))
    val start  = events.find(_.get("event_type").asText() == "spark_job_start").get
    start.get("action_context").asText() shouldBe "refresh-1/action-1"
    val stage = events.find(_.get("event_type").asText() == "spark_stage_completed").get
    start.get("stage_ids").elements().asScala.map(_.asInt()).toSeq should contain(stage.get("stage_id").asInt())
    events.exists(e =>
      e.get("event_type").asText() == "spark_job_end" &&
        e.get("job_id").asInt() == start.get("job_id").asInt()
    ) shouldBe true
  }
}
