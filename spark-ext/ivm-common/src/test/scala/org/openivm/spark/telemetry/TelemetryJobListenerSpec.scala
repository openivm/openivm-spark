package org.openivm.spark.telemetry

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class TelemetryJobListenerSpec extends AnyFunSpec with Matchers {
  it("correlates shared stages to jobs and stops reporting after their last owner ends") {
    val lines = ArrayBuffer.empty[String]
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
    val first = events.head
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
    val lines = ArrayBuffer.empty[String]
    val listener = new TelemetryJobListener(line => { lines += line; () })
    listener.recordJobStart(1, 0L, 0 until 300, "refresh/action", None)
    val event = new ObjectMapper().readTree(lines.head)
    event.get("stage_tracking_truncated").asBoolean() shouldBe true
    event.get("stage_ids").size() shouldBe 256
  }
}
