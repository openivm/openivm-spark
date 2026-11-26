package org.openivm.spark.telemetry

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.openivm.spark.common.{TimeTravelPinReason, TimeTravelPinStatus}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class OpenIvmDeepTelemetrySpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  private val Json  = new ObjectMapper()
  private val roots = ArrayBuffer.empty[File]

  private val DeepFields: Set[String] =
    OpenIvmTelemetryContract.DeepTextFields ++ OpenIvmTelemetryContract.DeepIntegralFields ++
      OpenIvmTelemetryContract.DeepArrayFields.keySet

  override protected def beforeEach(): Unit = {
    OpenIvmExecutionSpan.resetForTesting()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    OpenIvmExecutionSpan.resetForTesting()
    roots.foreach(deleteDir)
    roots.clear()
    super.afterEach()
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
    ()
  }

  private final class CapturingPublisher extends CompletedSpanPublisher {
    private val captured = ArrayBuffer.empty[String]

    override def publish(identity: OpenIvmTelemetryContract.ExecutionIdentity, payload: String): Path = {
      captured.synchronized(captured += payload)
      new Path("file:/captured/completed.json")
    }

    def raw: Seq[String] = captured.synchronized(captured.toVector)

    def single: JsonNode = {
      raw should have size 1
      Json.readTree(raw.head)
    }
  }

  private def identity(suffix: String): OpenIvmTelemetryContract.ExecutionIdentity =
    OpenIvmTelemetryContract.ExecutionIdentity(
      campaignId = "deep-campaign",
      requestId = s"deep-request-$suffix",
      correlationId = s"deep-correlation-$suffix",
      dbtNodeId = s"model.benchmark.deep_$suffix",
      materializedView = s"benchmark.deep_$suffix",
      operation = "refresh",
      phase = "refresh"
    )

  private def startSpan(suffix: String, deep: Boolean, publisher: CapturingPublisher): OpenIvmExecutionSpan =
    OpenIvmExecutionSpan.startDeepForTesting(identity(suffix), publisher, deepEnabled = deep)

  private def classify(span: OpenIvmExecutionSpan): Unit = {
    span.recordRefreshClassification(
      compileRefreshType = Some("AGGREGATE_GROUP"),
      effectiveRefreshType = Some("AGGREGATE_GROUP"),
      refreshReason = Some("kept")
    )
    span.recordTimeTravelPinStatus(TimeTravelPinStatus.Applied, TimeTravelPinReason.PinsResolved)
    span.recordSourceVersions(Seq(OpenIvmTelemetryContract.SourceVersion("benchmark.orders", 3L, 4L)))
    span.recordPendingDeltaCount(1L)
  }

  private def finish(span: OpenIvmExecutionSpan): Unit = {
    span.complete("incremental_executed", "driver-deep")
    span.emitIfNeeded("failed_before_end", "unused")
  }

  private def named(payload: JsonNode, field: String, key: String, name: String): JsonNode =
    payload
      .path(field)
      .elements()
      .asScala
      .find(_.path(key).asText() == name)
      .getOrElse(fail(s"missing $field entry $name"))

  private def phase(payload: JsonNode, name: String): JsonNode = named(payload, "phase_totals", "phase", name)

  private def counter(payload: JsonNode, name: String): Long =
    named(payload, "work_counters", "counter", name).path("value").asLong()

  private def fieldNames(payload: JsonNode): Set[String] = payload.fieldNames().asScala.toSet

  private def rendered(
      suffix: String,
      snapshot: OpenIvmExecutionSpan.DeepSnapshot,
      marker: String => String = OpenIvmExecutionSpan.markAugmentationFailed
  ): (OpenIvmTelemetryContract.ExecutionIdentity, ObjectNode) = {
    val publisher = new CapturingPublisher
    val span      = OpenIvmExecutionSpan.startForTesting(identity(suffix), publisher)
    classify(span)
    finish(span)
    val payload = OpenIvmExecutionSpan.withDeepBestEffort(publisher.raw.head, Some(snapshot), marker)
    (identity(suffix), Json.readTree(payload).asInstanceOf[ObjectNode])
  }

  private def reusable(accepted: OpenIvmTelemetryContract.ExecutionIdentity, payload: JsonNode): Boolean = {
    val root = new File(s"target/openivm-deep-${UUID.randomUUID().toString}")
    roots += root
    val exporter = new OpenIvmTelemetryExporter(root.toURI.toString, new Configuration())
    val raw      = Json.writeValueAsString(payload)
    exporter.publish(accepted, raw)
    exporter.reuseCompleted(accepted, raw)
  }

  describe("deep driver telemetry") {
    it("adds no deep fields and leaves the wire field set unchanged when deep is off") {
      val plain    = new CapturingPublisher
      val off      = new CapturingPublisher
      val baseline = OpenIvmExecutionSpan.startForTesting(identity("off_a"), plain)
      classify(baseline)
      finish(baseline)
      OpenIvmExecutionSpan.resetForTesting()

      val span   = startSpan("off_b", deep = false, off)
      var ran    = 0
      val result = OpenIvmExecutionSpan.measureActivePhase("register_stage_view_ddl") { ran += 1; 42 }
      OpenIvmExecutionSpan.recordActiveWork("register.sources", 3L)
      span.measureDeepPhase("perf_register_views")(ran += 1)
      span.recordDeepExternalPhase("perf_register_views", 9L)
      span.recordDeepTimer("delta.commit.attempt", 9L)
      OpenIvmExecutionSpan.deepActive shouldBe false
      classify(span)
      finish(span)

      result shouldBe 42
      ran shouldBe 2
      fieldNames(off.single).intersect(DeepFields) shouldBe empty
      fieldNames(off.single) shouldBe fieldNames(plain.single)
    }

    it("emits deep fields that the completed-object validator accepts and rejects when malformed") {
      val publisher = new CapturingPublisher
      val span      = startSpan("valid", deep = true, publisher)
      OpenIvmExecutionSpan.deepActive shouldBe true
      OpenIvmExecutionSpan.measureActivePhase("register_source_delta")(Thread.sleep(2L))
      OpenIvmExecutionSpan.recordActiveWork("register.stage_parts", 2L)
      OpenIvmExecutionSpan.recordActiveWork("cleanup.delta_views", -1L)
      span.recordDeepTimer("delta.commit.attempt", 5000L)
      span.recordDeepSkippedPhase("finalize_mark_consumed")
      classify(span)
      finish(span)

      val payload = publisher.single
      payload.path("clock_domain_id").asText() should not be empty
      payload.path("owner_thread_coverage_status").asText() shouldBe OpenIvmTelemetryContract.DeepCoverageComplete
      payload.path("duration_ns").asLong() should be >= payload.path("owner_thread_wrapped_ns").asLong()
      payload.path("owner_thread_unattributed_ns").asLong() shouldBe
        payload.path("duration_ns").asLong() - payload.path("owner_thread_wrapped_ns").asLong()
      phase(payload, "register_source_delta").path("owner_top_level_count").asLong() shouldBe 1L
      phase(payload, "finalize_mark_consumed").path("skipped_count").asLong() shouldBe 1L
      counter(payload, "register.stage_parts") shouldBe 2L
      counter(payload, "cleanup.delta_views") shouldBe -1L
      named(payload, "timer_totals", "timer", "delta.commit.attempt").path("total_ns").asLong() shouldBe 5000L

      val root = new File(s"target/openivm-deep-${UUID.randomUUID().toString}")
      roots += root
      val exporter = new OpenIvmTelemetryExporter(root.toURI.toString, new Configuration())
      val accepted = identity("valid")
      exporter.publish(accepted, publisher.raw.head)
      exporter.reuseCompleted(accepted, publisher.raw.head) shouldBe true

      val negative = payload.deepCopy[ObjectNode]()
      negative.path("phase_totals").get(0).asInstanceOf[ObjectNode].put("wrapped_ns", -1L)
      an[OpenIvmTelemetryExportException] should be thrownBy
        exporter.reuseCompleted(accepted, Json.writeValueAsString(negative))
      val extraKey = payload.deepCopy[ObjectNode]()
      extraKey.path("work_counters").get(0).asInstanceOf[ObjectNode].put("unit", 1L)
      an[OpenIvmTelemetryExportException] should be thrownBy
        exporter.reuseCompleted(accepted, Json.writeValueAsString(extraKey))
    }

    it("feeds coverage only from owner-thread top-level wrappers, not nested, imported or off-thread work") {
      val publisher = new CapturingPublisher
      val span      = startSpan("nested", deep = true, publisher)
      OpenIvmExecutionSpan.measureActivePhase("perf_register_views") {
        Thread.sleep(3L)
        OpenIvmExecutionSpan.measureActivePhase("register_source_delta")(Thread.sleep(3L))
      }
      span.recordDeepExternalPhase("perf_register_views", 10000L)
      val pool = Executors.newSingleThreadExecutor()
      try {
        pool
          .submit(new Runnable {
            override def run(): Unit = {
              OpenIvmExecutionSpan.measureActivePhase("executor_passthrough")(())
              span.measureDeepPhase("parallel_publication")(Thread.sleep(3L))
            }
          })
          .get(30, TimeUnit.SECONDS)
      } finally pool.shutdownNow()
      classify(span)
      finish(span)

      val payload = publisher.single
      val outer   = phase(payload, "perf_register_views")
      val inner   = phase(payload, "register_source_delta")
      outer.path("owner_top_level_count").asLong() shouldBe 1L
      outer.path("external_count").asLong() shouldBe 1L
      outer.path("external_ms").asLong() shouldBe 10000L
      inner.path("wrapped_count").asLong() shouldBe 1L
      inner.path("owner_top_level_count").asLong() shouldBe 0L
      phase(payload, "parallel_publication").path("off_thread_count").asLong() shouldBe 1L
      payload.path("phase_totals").elements().asScala.map(_.path("phase").asText()).toSet should not contain
        "executor_passthrough"
      payload.path("owner_thread_wrapped_ns").asLong() shouldBe outer.path("wrapped_ns").asLong()
      payload.path("owner_thread_wrapped_ns").asLong() should be <= payload.path("duration_ns").asLong()
      payload.path("owner_thread_coverage_status").asText() shouldBe OpenIvmTelemetryContract.DeepCoverageComplete
    }

    it("reports incomplete coverage with no unattributed figure when a wrapper is active at freeze") {
      val publisher = new CapturingPublisher
      val span      = startSpan("active", deep = true, publisher)
      classify(span)
      an[IllegalStateException] should be thrownBy
        OpenIvmExecutionSpan.measureActivePhase("profile_flush_persist") {
          finish(span)
          throw new IllegalStateException("persist failed")
        }
      OpenIvmExecutionSpan.recordActiveWork("after.freeze", 1L)

      val payload = publisher.single
      payload.path("owner_thread_coverage_status").asText() shouldBe OpenIvmTelemetryContract.DeepCoverageIncomplete
      payload.path("owner_thread_coverage_reason").asText() shouldBe
        OpenIvmTelemetryContract.DeepActiveWrappersAtFreeze
      payload.has("owner_thread_unattributed_ns") shouldBe false
      payload.path("active_wrappers_at_freeze").asLong() shouldBe 1L
      payload.path("phase_totals").size() shouldBe 0
      payload.path("work_counters").size() shouldBe 0
    }

    it("splits owner wrapper time that runs after the engine completion instant") {
      val publisher = new CapturingPublisher
      val span      = startSpan("tail", deep = true, publisher)
      classify(span)
      OpenIvmExecutionSpan.measureActivePhase("create_post_completion") {
        Thread.sleep(2L)
        span.complete("incremental_executed", "driver-deep")
        Thread.sleep(5L)
      }
      span.emitIfNeeded("failed_before_end", "unused")

      val payload = publisher.single
      val wrapped = phase(payload, "create_post_completion").path("wrapped_ns").asLong()
      val inside  = payload.path("owner_thread_wrapped_ns").asLong()
      val tail    = payload.path("owner_thread_post_completion_wrapped_ns").asLong()
      tail should be >= TimeUnit.MILLISECONDS.toNanos(4L)
      inside should be <= payload.path("duration_ns").asLong()
      (inside + tail) shouldBe wrapped
      payload.path("post_completion_ns").asLong() should be >= tail
      payload.path("owner_thread_coverage_status").asText() shouldBe OpenIvmTelemetryContract.DeepCoverageComplete
    }

    it("gates concurrent spans by their own captured deep setting") {
      val onPublisher  = new CapturingPublisher
      val offPublisher = new CapturingPublisher
      val deepStarted  = new CountDownLatch(1)
      val pool         = Executors.newFixedThreadPool(2)
      try {
        val on = pool.submit(new Runnable {
          override def run(): Unit = {
            val span = startSpan("concurrent_on", deep = true, onPublisher)
            deepStarted.countDown()
            OpenIvmExecutionSpan.measureActivePhase("register_source_delta")(())
            OpenIvmExecutionSpan.recordActiveWork("register.sources")
            classify(span)
            finish(span)
          }
        })
        val off = pool.submit(new Runnable {
          override def run(): Unit = {
            deepStarted.await(30, TimeUnit.SECONDS)
            val span = startSpan("concurrent_off", deep = false, offPublisher)
            OpenIvmExecutionSpan.deepActive shouldBe false
            OpenIvmExecutionSpan.measureActivePhase("register_source_delta")(())
            OpenIvmExecutionSpan.recordActiveWork("register.sources")
            classify(span)
            finish(span)
          }
        })
        on.get(30, TimeUnit.SECONDS)
        off.get(30, TimeUnit.SECONDS)
      } finally pool.shutdownNow()

      fieldNames(offPublisher.single).intersect(DeepFields) shouldBe empty
      counter(onPublisher.single, "register.sources") shouldBe 1L
    }

    it("bounds phases, counters and names and reports drops and truncation instead of growing") {
      val publisher = new CapturingPublisher
      val span      = startSpan("caps", deep = true, publisher)
      (1 to OpenIvmTelemetryContract.DeepPhaseTotalsCap + 3).foreach { i =>
        OpenIvmExecutionSpan.measureActivePhase(s"phase_$i")(())
      }
      (1 to OpenIvmTelemetryContract.DeepWorkCountersCap + 2).foreach { i =>
        OpenIvmExecutionSpan.recordActiveWork(s"counter.$i")
      }
      span.recordDeepTimer("X" * (OpenIvmTelemetryContract.DeepNameMaxLength + 40), 1L)
      span.recordDeepTimer("Delta Commit/Attempt", 1L)
      classify(span)
      finish(span)

      val payload = publisher.single
      payload.path("phase_totals").size() shouldBe OpenIvmTelemetryContract.DeepPhaseTotalsCap
      payload.path("phase_totals_dropped_count").asLong() shouldBe 3L
      payload.path("work_counters").size() shouldBe OpenIvmTelemetryContract.DeepWorkCountersCap
      payload.path("work_counters_dropped_count").asLong() shouldBe 2L
      payload.path("deep_names_truncated_count").asLong() shouldBe 1L
      payload.path("timer_totals").elements().asScala.map(_.path("timer").asText()).toSeq shouldBe
        Seq("x" * OpenIvmTelemetryContract.DeepNameMaxLength, "delta_commit_attempt")
      publisher.raw.head.length should be < 262144
    }

    it("reports inconsistent overcoverage with no unattributed figure when owner wrappers exceed the span") {
      val state = new OpenIvmExecutionSpan.DeepState(1000L, Thread.currentThread())
      state.exit("completion_race", state.enter(), 1000L, 1600L, failed = false)
      state.markCompleted(1400L)
      val (accepted, payload) = rendered("inconsistent", state.freeze(1400L))

      payload.path("owner_thread_coverage_status").asText() shouldBe OpenIvmTelemetryContract.DeepCoverageInconsistent
      payload.path("owner_thread_coverage_reason").asText() shouldBe OpenIvmTelemetryContract.DeepWrappedExceedsSpan
      payload.path("duration_ns").asLong() shouldBe 400L
      payload.path("owner_thread_wrapped_ns").asLong() shouldBe 600L
      payload.path("owner_thread_overcoverage_ns").asLong() shouldBe 200L
      payload.has("owner_thread_unattributed_ns") shouldBe false
      phase(payload, "completion_race").path("owner_top_level_count").asLong() shouldBe 1L
      reusable(accepted, payload) shouldBe true

      val withResidual = payload.deepCopy().put("owner_thread_unattributed_ns", 0L)
      an[OpenIvmTelemetryExportException] should be thrownBy reusable(accepted, withResidual)
      val unknownStatus = payload.deepCopy().put("owner_thread_coverage_status", "partial")
      an[OpenIvmTelemetryExportException] should be thrownBy reusable(accepted, unknownStatus)
    }

    it("marks a deep-requested span whose augmentation failed instead of passing as deep-off") {
      val registry = new MetricRegistry()
      BoundedTelemetryRuntime.registerMetrics(registry) shouldBe true
      def gauge(name: String): Long =
        registry.getGauges.get(s"openivm.telemetry.deep.$name").getValue.asInstanceOf[java.lang.Number].longValue()
      val failuresBefore = gauge("augmentation_failures")
      val unmarkedBefore = gauge("unmarked_payloads")

      val state               = new OpenIvmExecutionSpan.DeepState(1000L, Thread.currentThread())
      val broken              = state.freeze(1400L).copy(phases = null)
      val (accepted, payload) = rendered("augmentation_failed", broken)

      payload.path("deep_status").asText() shouldBe OpenIvmTelemetryContract.DeepStatusAugmentationFailed
      fieldNames(payload).intersect(DeepFields) shouldBe Set("deep_status")
      reusable(accepted, payload) shouldBe true
      gauge("augmentation_failures") shouldBe failuresBefore + 1L
      gauge("unmarked_payloads") shouldBe unmarkedBefore

      val mixed = payload.deepCopy().put("owner_thread_coverage_status", OpenIvmTelemetryContract.DeepCoverageComplete)
      mixed.put("owner_thread_unattributed_ns", 0L)
      an[OpenIvmTelemetryExportException] should be thrownBy reusable(accepted, mixed)
      val withClock = payload.deepCopy().put("clock_domain_id", "clock")
      an[OpenIvmTelemetryExportException] should be thrownBy reusable(accepted, withClock)
      val withPhases = payload.deepCopy()
      withPhases.putArray("phase_totals")
      an[OpenIvmTelemetryExportException] should be thrownBy reusable(accepted, withPhases)

      val (fallbackAccepted, fallback) =
        rendered("augmentation_marker_failed", broken, _ => throw new IllegalStateException("marker"))
      fallback.path("deep_status").asText() shouldBe OpenIvmTelemetryContract.DeepStatusAugmentationFailed
      fieldNames(fallback).intersect(DeepFields) shouldBe Set("deep_status")
      reusable(fallbackAccepted, fallback) shouldBe true
      gauge("augmentation_failures") shouldBe failuresBefore + 2L
      gauge("unmarked_payloads") shouldBe unmarkedBefore

      val minimal = Json.readTree(OpenIvmExecutionSpan.withDeepBestEffort("unrenderable", Some(broken)))
      minimal.path("deep_status").asText() shouldBe OpenIvmTelemetryContract.DeepStatusAugmentationFailed
      fieldNames(minimal) shouldBe Set("deep_status")
      an[OpenIvmTelemetryExportException] should be thrownBy reusable(accepted, minimal)
      gauge("augmentation_failures") shouldBe failuresBefore + 3L
      gauge("unmarked_payloads") shouldBe unmarkedBefore + 1L
    }

    it("emits a marked last-resort diagnostic for invalid inputs while deep-off stays byte-identical") {
      val state          = new OpenIvmExecutionSpan.DeepState(1000L, Thread.currentThread())
      val broken         = state.freeze(1400L).copy(phases = null)
      val inputs         = Seq(null, "", " \t", "[]", "null", "{broken}", "{} {}", """{"clock_domain_id":"clock"}""")
      val failuresBefore = OpenIvmExecutionSpan.deepAugmentationFailureCount
      val unmarkedBefore = OpenIvmExecutionSpan.deepUnmarkedPayloadCount

      inputs.foreach { input =>
        val raw = OpenIvmExecutionSpan.withDeepBestEffort(
          input,
          Some(broken),
          _ => throw new IllegalStateException("marker")
        )
        val payload = Json.readTree(raw)
        payload.isObject shouldBe true
        fieldNames(payload) shouldBe Set("deep_status")
        payload.path("deep_status").asText() shouldBe OpenIvmTelemetryContract.DeepStatusAugmentationFailed

        OpenIvmExecutionSpan.withDeepBestEffort(input, None, _ => fail("deep-off called marker")) shouldBe input
      }
      OpenIvmExecutionSpan.deepAugmentationFailureCount shouldBe failuresBefore + inputs.size
      OpenIvmExecutionSpan.deepUnmarkedPayloadCount shouldBe unmarkedBefore + inputs.size
    }
  }
}
