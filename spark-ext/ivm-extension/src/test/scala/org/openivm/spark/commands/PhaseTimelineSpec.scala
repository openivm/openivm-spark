package org.openivm.spark.commands

import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.openivm.spark.telemetry.BoundedTelemetryQueue
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class PhaseTimelineSpec extends AnyFunSpec with Matchers {
  it("reports exclusive time from direct nested intervals and retains failure context") {
    val events = ArrayBuffer.empty[String]
    val times  = Iterator(0L, 10L, 15L, 25L, 30L, 40L, 60L, 100L)
    val timeline =
      new PhaseTimeline("refresh-1", "db.mv", "refresh", line => { events += line; () }, () => times.next(), () => 0L)
    timeline.time("outer", "") {
      timeline.time("middle", "") {
        timeline.time("inner", "") {}
      }
      intercept[IllegalStateException] {
        timeline.time("failed", "") { throw new IllegalStateException("test") }
      }
    }

    val mapper = new ObjectMapper()
    val rows = events.map(line => mapper.readTree(line))
      .filter(_.get("event_type").asText() == "phase")
      .map(row => row.get("phase").asText() -> row).toMap
    rows("outer").get("inclusive_ns").asLong() shouldBe 100L
    rows("outer").get("exclusive_ns").asLong() shouldBe 60L
    rows("middle").get("inclusive_ns").asLong() shouldBe 20L
    rows("middle").get("exclusive_ns").asLong() shouldBe 10L
    rows("inner").get("parent_phase_id").asLong() shouldBe rows("middle").get("phase_id").asLong()
    rows("failed").get("parent_phase_id").asLong() shouldBe rows("outer").get("phase_id").asLong()
    rows("failed").get("outcome").asText() shouldBe "failed"
  }

  it("publishes a correlated phase start before the body runs and closes failed phases") {
    val events = ArrayBuffer.empty[String]
    val mapper = new ObjectMapper()
    val timeline = new PhaseTimeline(
      "refresh-1",
      "db.mv",
      "refresh",
      line => { events += line; () },
      requestId = Some("request-1")
    )
    intercept[IllegalStateException] {
      timeline.time("load", "source") {
        events.size shouldBe 1
        val started = mapper.readTree(events.head)
        started.get("event_type").asText() shouldBe "phase_started"
        started.get("request_id").asText() shouldBe "request-1"
        started.get("outcome").asText() shouldBe "running"
        started.has("inclusive_ns") shouldBe false
        throw new IllegalStateException("test")
      }
    }
    val started  = mapper.readTree(events.head)
    val finished = mapper.readTree(events.last)
    finished.get("event_type").asText() shouldBe "phase"
    finished.get("phase_id").asLong() shouldBe started.get("phase_id").asLong()
    finished.get("outcome").asText() shouldBe "failed"
  }

  it("publishes the phase start while the operation is still running") {
    val root = Files.createTempDirectory("openivm-phase-live-")
    val conf = new SparkConf(false)
      .set(BoundedTelemetryQueue.EnabledKey, "true")
      .set(BoundedTelemetryQueue.LocalUriKey, root.toUri.toString)
    val queue   = BoundedTelemetryQueue.configured(conf, new Configuration()).get
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val error   = new AtomicReference[Throwable]()
    val timeline = new PhaseTimeline(
      "refresh-1",
      "db.mv",
      "refresh",
      line => { queue.offer(line); () },
      requestId = Some("request-1")
    )
    val operation = new Thread(() => {
      try timeline.time("load", "source") { entered.countDown(); release.await(); () }
      catch { case NonFatal(e) => error.set(e) }
    })
    try {
      operation.start()
      entered.await(10, TimeUnit.SECONDS) shouldBe true
      queue.awaitDrain(10000) shouldBe true
      operation.isAlive shouldBe true
      val records = root.toFile.listFiles().filter(_.getName.endsWith(".jsonl"))
      records.length shouldBe 1
      val started = new ObjectMapper().readTree(records.head)
      started.get("event_type").asText() shouldBe "phase_started"
      started.get("request_id").asText() shouldBe "request-1"
    } finally {
      release.countDown()
      operation.join(10000)
      queue.shutdown(10000) shouldBe true
      Option(root.toFile.listFiles()).foreach(_.foreach(_.delete()))
      Files.deleteIfExists(root)
    }
    operation.isAlive shouldBe false
    Option(error.get()) shouldBe None
  }

  it("reports externally measured intervals without inventing exclusive time") {
    val events   = ArrayBuffer.empty[String]
    val timeline = new PhaseTimeline("refresh-1", "db.mv", "refresh", line => { events += line; () })
    timeline.external("state_barrier_wait", 0L, 10L, 30L, succeeded = true)
    timeline.external("state_barrier_hold", 1L, 30L, 90L, succeeded = false)

    val mapper = new ObjectMapper()
    val rows   = events.map(line => mapper.readTree(line))
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
