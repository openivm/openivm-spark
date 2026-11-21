package org.openivm.spark.commands

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class PhaseTimelineSpec extends AnyFunSpec with Matchers {
  it("reports exclusive time from direct nested intervals and retains failure context") {
    val events = ArrayBuffer.empty[String]
    val times = Iterator(0L, 10L, 15L, 25L, 30L, 40L, 60L, 100L)
    val timeline = new PhaseTimeline("refresh-1", "db.mv", "refresh", line => { events += line; () },
      () => times.next(), () => 0L)
    timeline.time("outer", "") {
      timeline.time("middle", "") {
        timeline.time("inner", "") {}
      }
      intercept[IllegalStateException] {
        timeline.time("failed", "") { throw new IllegalStateException("test") }
      }
    }

    val mapper = new ObjectMapper()
    val rows = events.map(line => mapper.readTree(line)).map(row => row.get("phase").asText() -> row).toMap
    rows("outer").get("inclusive_ns").asLong() shouldBe 100L
    rows("outer").get("exclusive_ns").asLong() shouldBe 60L
    rows("middle").get("inclusive_ns").asLong() shouldBe 20L
    rows("middle").get("exclusive_ns").asLong() shouldBe 10L
    rows("inner").get("parent_phase_id").asLong() shouldBe rows("middle").get("phase_id").asLong()
    rows("failed").get("parent_phase_id").asLong() shouldBe rows("outer").get("phase_id").asLong()
    rows("failed").get("outcome").asText() shouldBe "failed"
  }

  it("reports externally measured intervals without inventing exclusive time") {
    val events = ArrayBuffer.empty[String]
    val timeline = new PhaseTimeline("refresh-1", "db.mv", "refresh", line => { events += line; () })
    timeline.external("state_barrier_wait", 0L, 10L, 30L, succeeded = true)
    timeline.external("state_barrier_hold", 1L, 30L, 90L, succeeded = false)

    val mapper = new ObjectMapper()
    val rows = events.map(line => mapper.readTree(line))
    rows.map(_.get("phase").asText()) shouldBe Seq("state_barrier_wait", "state_barrier_hold")
    rows.map(_.get("inclusive_ns").asLong()) shouldBe Seq(20L, 60L)
    rows.foreach { row =>
      row.has("exclusive_ns") shouldBe false
      row.get("exclusive_scope").asText() shouldBe "unavailable_overlapping_interval"
      row.get("refresh_id").asText() shouldBe "refresh-1"
    }
    rows.last.get("outcome").asText() shouldBe "failed"
  }
}
