package org.openivm.spark.commands

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{MonotonicClockDomain, MvCatalog, OpenIvmGarbageCollector, StagingCatalog}
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, OpenIvmExecutionSpan}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class GarbageCollectionTelemetrySpec extends AnyFunSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _
  private val warehouse = new File(s"target/test-warehouse-gc-telemetry-${UUID.randomUUID().toString.take(8)}")
  private val mapper = new ObjectMapper()

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-spark-GarbageCollectionTelemetrySpec")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.openivm.enabled", "true")
      .config(BoundedTelemetryQueue.EnabledKey, "true")
      .config(BoundedTelemetryQueue.LocalUriKey, new File(warehouse, "telemetry").toURI.toString)
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    MvCatalog.ensureTables(spark)
    StagingCatalog.ensureTables(spark)
  }

  override def afterAll(): Unit = {
    OpenIvmGarbageCollector.resetTelemetryOfferForTesting()
    if (spark != null) spark.stop()
    if (warehouse.exists()) {
      val paths = Files.walk(warehouse.toPath)
      try paths.iterator().asScala.toSeq.reverse.foreach(Files.delete)
      finally paths.close()
    }
  }

  describe("GC telemetry with the queue enabled") {
    it("emits correlated plan and cleanup events without exposing paths") {
      val originalOpId = spark.sparkContext.getLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty)
      val events = ArrayBuffer.empty[String]
      try {
        spark.sparkContext.getConf.getBoolean(BoundedTelemetryQueue.EnabledKey, false) shouldBe true
        OpenIvmGarbageCollector.setTelemetryOfferForTesting(line => { events += line; () })
        spark.sparkContext.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, "gc-operation-test")
        OpenIvmGarbageCollector.prepare(spark)
        events should have size 1
        val event = mapper.readTree(events.head)
        event.get("event_type").asText() shouldBe "openivm_gc"
        event.get("clock_domain_id").asText() shouldBe MonotonicClockDomain.id
        event.get("scope").asText() shouldBe "garbage_collection"
        event.get("phase").asText() shouldBe "plan"
        event.get("operation_id").asText() shouldBe "gc-operation-test"
        event.get("context_status").asText() shouldBe "present"
        java.time.Instant.parse(event.get("start_utc").asText())
        event.get("inclusive_duration_ns").asLong() should be >= 0L
        event.get("tombstone_count").asLong() should be >= 0L
        event.get("reference_count").asLong() should be >= 0L
        events.head.length should be < 4096

        OpenIvmGarbageCollector.setSweepClockForTesting(System.currentTimeMillis())
        try {
          val tombstone = "gc-telemetry-test-tombstone"
          OpenIvmGarbageCollector.collect(spark, Some(OpenIvmGarbageCollector.Plan(Set(tombstone), Set(tombstone))))
        } finally OpenIvmGarbageCollector.setSweepClockForTesting(0L)
        events should have size 2
        val cleanup = mapper.readTree(events.last)
        cleanup.get("phase").asText() shouldBe "cleanup"
        cleanup.get("operation_id").asText() shouldBe "gc-operation-test"
        cleanup.get("clock_domain_id").asText() shouldBe MonotonicClockDomain.id
        cleanup.get("tombstone_count").asLong() shouldBe 1L
        cleanup.get("reference_count").asLong() shouldBe 1L
        cleanup.get("tombstones_deleted_count").asLong() shouldBe 0L
        cleanup.get("orphan_sweep_scheduled").asLong() shouldBe 0L
        events.last should not include "gc-telemetry-test-tombstone"
      } finally {
        spark.sparkContext.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, originalOpId)
        OpenIvmGarbageCollector.resetTelemetryOfferForTesting()
      }
    }

    it("marks absent operation context explicitly") {
      val originalOpId = spark.sparkContext.getLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty)
      val events = ArrayBuffer.empty[String]
      try {
        spark.sparkContext.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, null)
        OpenIvmGarbageCollector.setTelemetryOfferForTesting(line => { events += line; () })
        val expectedPlan = OpenIvmGarbageCollector.prepare(spark)
        events should have size 1
        val event = mapper.readTree(events.head)
        event.get("context_status").asText() shouldBe "missing"
        event.get("operation_id").isNull shouldBe true
        event.get("clock_domain_id").asText() shouldBe MonotonicClockDomain.id
        OpenIvmGarbageCollector.prepare(spark) shouldBe expectedPlan
      } finally {
        spark.sparkContext.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, originalOpId)
        OpenIvmGarbageCollector.resetTelemetryOfferForTesting()
      }
    }
  }
}
