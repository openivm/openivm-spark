package org.openivm.spark.telemetry

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BoundedTelemetryQueueSpec extends AnyFunSpec with Matchers {
  private val json = new ObjectMapper()
  private def remove(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(remove))
    file.delete()
    ()
  }

  describe("bounded telemetry queue") {
    it("is disabled by default and publishes valid JSONL and a UTC manifest when enabled") {
      val root = new File(s"target/queue-spec-${UUID.randomUUID()}")
      try {
        val conf = new SparkConf(false)
        BoundedTelemetryQueue.configured(conf, new Configuration()) shouldBe None
        root.exists() shouldBe false
        conf.set(BoundedTelemetryQueue.EnabledKey, "true")
        conf.set(BoundedTelemetryQueue.UriKey, root.toURI.toString)
        val q = BoundedTelemetryQueue.configured(conf, new Configuration()).get
        q.offer("OPENIVM_EXECUTION_SPAN {\"duration_ms\":3}") shouldBe true
        q.shutdown(10000) shouldBe true
        q.health.published shouldBe 1L
        val files   = root.listFiles().toSeq
        val records = files.filter(_.getName.endsWith(".jsonl"))
        records.size shouldBe 1
        val data = new String(Files.readAllBytes(records.head.toPath), StandardCharsets.UTF_8)
        json.readTree(data).get("duration_ms").asInt() shouldBe 3
        val manifest = files.find(_.getName.endsWith("-manifest.json")).get
        val m        = json.readTree(Files.readAllBytes(manifest.toPath))
        m.get("published").asLong() shouldBe 1L
        m.get("started_at").asText().endsWith("Z") shouldBe true
        m.get("finished_at").asText().endsWith("Z") shouldBe true
      } finally remove(root)
    }

    it("drops immediately when saturated and accounts for failures and shutdown") {
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val writer = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit = {
          entered.countDown()
          release.await()
          throw new IllegalStateException("unavailable")
        }
        override def manifest(value: String): Unit = ()
      }
      val other = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = ()
        override def manifest(value: String): Unit = ()
      }
      val conf = new Configuration(false)
      val q = new BoundedTelemetryQueue(
        1,
        100,
        writer,
        Some(BoundedTelemetryQueue.Destination("file:/local", conf)),
        Seq(other -> BoundedTelemetryQueue.Destination("abfss://fs@account/path", conf))
      )
      try {
        q.offer("{}") shouldBe true
        entered.await(10, TimeUnit.SECONDS) shouldBe true
        q.offer("{}") shouldBe true
        q.offer("{}") shouldBe false
        q.offer("x" * 101) shouldBe false
        q.shutdown(1) shouldBe false
        q.offer("{}") shouldBe false
      } finally release.countDown()
      q.shutdown(10000) shouldBe true
      val h = q.health
      h.accepted shouldBe 2L
      h.exportFailures shouldBe 2L
      h.droppedFull shouldBe 1L
      h.droppedOversized shouldBe 1L
      h.droppedClosed shouldBe 1L
      h.published shouldBe 0L
      h.destinations("local").dropped shouldBe 3L
      h.destinations("abfss").dropped shouldBe 3L
      h.destinations("local").exportFailures shouldBe 2L
      h.destinations("abfss").published shouldBe 2L
    }

    it("counts malformed JSON and a failed manifest without exposing exception content") {
      val writer = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = fail("invalid JSON was published")
        override def manifest(value: String): Unit = throw new IllegalStateException("secret")
      }

      it("waits for in-flight records without closing the queue") {
        val entered = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val writer = new BoundedTelemetryQueue.Writer {
          override def record(line: String): Unit    = { entered.countDown(); release.await(); () }
          override def manifest(value: String): Unit = ()
        }
        val q = new BoundedTelemetryQueue(2, 100, writer)
        try {
          q.offer("{}") shouldBe true
          entered.await(10, TimeUnit.SECONDS) shouldBe true
          q.health.queued shouldBe 0
          q.awaitDrain(10) shouldBe false
          release.countDown()
          q.awaitDrain(10000) shouldBe true
          q.offer("{}") shouldBe true
          q.awaitDrain(10000) shouldBe true
        } finally {
          release.countDown()
          q.shutdown(10000) shouldBe true
        }
      }
      val q = new BoundedTelemetryQueue(2, 100, writer)
      q.offer("not json") shouldBe true
      q.shutdown(10000) shouldBe true
      q.health.exportFailures shouldBe 1L
      q.health.manifestFailures shouldBe 1L
      q.health.toString.contains("secret") shouldBe false
    }
    it("validates opt-in settings without constructing a queue") {
      val conf = new SparkConf(false)
      conf.set(BoundedTelemetryQueue.EnabledKey, "invalid")
      intercept[IllegalArgumentException] {
        BoundedTelemetryQueue.configured(conf, new Configuration())
      }
      conf.set(BoundedTelemetryQueue.EnabledKey, "true")
      intercept[IllegalArgumentException] {
        BoundedTelemetryQueue.configured(conf, new Configuration())
      }
      conf.set(BoundedTelemetryQueue.UriKey, "abfss:///missing-authority")
      intercept[IllegalArgumentException] {
        BoundedTelemetryQueue.configured(conf, new Configuration())
      }
      conf.set(BoundedTelemetryQueue.UriKey, "file:/queue-test")
      conf.set(BoundedTelemetryQueue.CapacityKey, "0")
      intercept[IllegalArgumentException] {
        BoundedTelemetryQueue.configured(conf, new Configuration())
      }
    }

    it("makes the plugin-owned queue available to the span bridge until shutdown") {
      val root = new File(s"target/queue-runtime-${UUID.randomUUID()}")
      val conf = new SparkConf(false)
      conf.set(BoundedTelemetryQueue.EnabledKey, "true")
      conf.set(BoundedTelemetryQueue.UriKey, root.toURI.toString)
      try {
        val q = BoundedTelemetryRuntime.start(conf, new Configuration()).get
        try BoundedTelemetryRuntime.offer("{\"duration_ms\":5}")
        finally BoundedTelemetryRuntime.stop(q, 10000) shouldBe true
        BoundedTelemetryRuntime.offer("{\"duration_ms\":6}")
        BoundedTelemetryRuntime.health.get.accepted shouldBe 1L
        BoundedTelemetryRuntime.health.get.published shouldBe 1L
      } finally remove(root)
    }
    it("tracks timed-out workers and fences registration with a topology epoch") {
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val writer = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = { entered.countDown(); release.await(); () }
        override def manifest(value: String): Unit = ()
      }
      val conf = new Configuration(false)
      conf.set("openivm.test.marker", "copied")
      val q = new BoundedTelemetryQueue(
        1,
        100,
        writer,
        Some(BoundedTelemetryQueue.Destination("file:/queue-sink", conf)),
        Seq(writer -> BoundedTelemetryQueue.Destination("abfss://fs@account/queue-sink", conf))
      )
      val before = BoundedTelemetryRuntime.withSinkTopology((epoch: Long) => epoch)
      try {
        BoundedTelemetryRuntime.startWith(Some(q)) shouldBe Some(q)
        q.offer("{}") shouldBe true
        entered.await(10, TimeUnit.SECONDS) shouldBe true
        BoundedTelemetryRuntime.stop(q, 1) shouldBe false
        BoundedTelemetryRuntime.sinksDisjoint { (uri, actualConf) =>
          uri != "file:/queue-sink" || actualConf.get("openivm.test.marker") != "copied"
        } shouldBe false
        BoundedTelemetryRuntime.sinksDisjoint { (uri, _) => !uri.startsWith("abfss:") } shouldBe false
        BoundedTelemetryRuntime.sinksDisjoint { (_, _) => throw new IllegalStateException("bad check") } shouldBe false
        BoundedTelemetryRuntime.startWith(None) shouldBe None
        BoundedTelemetryRuntime.withSinkTopology((epoch: Long) => epoch) should be > before
      } finally {
        release.countDown()
        BoundedTelemetryRuntime.stop(q, 10000)
      }
      BoundedTelemetryRuntime.sinksDisjoint((_, _) => false) shouldBe true
    }
    it("exports to both destinations independently and reports partial failures in the manifest") {
      var cloudManifest = ""
      val failedLocal = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = throw new IllegalStateException("local unavailable")
        override def manifest(value: String): Unit = throw new IllegalStateException("local unavailable")
      }
      val cloud = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = ()
        override def manifest(value: String): Unit = { cloudManifest = value }
      }
      val conf = new Configuration(false)
      val q = new BoundedTelemetryQueue(
        1,
        100,
        failedLocal,
        Some(BoundedTelemetryQueue.Destination("file:/local", conf)),
        Seq(cloud -> BoundedTelemetryQueue.Destination("abfss://fs@account/path", conf))
      )
      q.offer("{}") shouldBe true
      q.shutdown(10000) shouldBe true
      val h = q.health
      h.accepted shouldBe 1L
      h.published shouldBe 0L
      h.exportFailures shouldBe 1L
      h.destinations("local").exportFailures shouldBe 1L
      h.destinations("local").manifestFailures shouldBe 1L
      h.destinations("abfss").published shouldBe 1L
      val m = json.readTree(cloudManifest)
      m.path("destinations").path("local").path("manifest_failures").asLong() shouldBe 1L
      m.path("destinations").path("abfss").path("published").asLong() shouldBe 1L
    }

    it("validates legacy versus explicit destination configuration") {
      val conf = new SparkConf(false)
      conf.set(BoundedTelemetryQueue.EnabledKey, "true")
      conf.set(BoundedTelemetryQueue.LocalUriKey, "file:/local")
      conf.set(BoundedTelemetryQueue.OneLakeUriKey, "abfss://fs@account/path")
      val paths = BoundedTelemetryQueue.configuredDestinationUris(conf)
      paths.size shouldBe 2
      paths.exists(_.startsWith("file:")) shouldBe true
      paths.exists(_.startsWith("abfss:")) shouldBe true
      conf.set(BoundedTelemetryQueue.UriKey, "file:/legacy")
      intercept[IllegalArgumentException] { BoundedTelemetryQueue.configuredDestinationUris(conf) }
      conf.remove(BoundedTelemetryQueue.UriKey)
      conf.set(BoundedTelemetryQueue.LocalUriKey, "abfss://fs@account/path")
      intercept[IllegalArgumentException] { BoundedTelemetryQueue.configuredDestinationUris(conf) }
    }
    it("exports destination counters and configuration failures through fixed registry names") {
      val registry = new MetricRegistry()
      BoundedTelemetryRuntime.registerMetrics(registry) shouldBe true
      def gauge(name: String): Long =
        registry.getGauges.get(s"openivm.telemetry.queue.$name").getValue.asInstanceOf[java.lang.Number].longValue()
      val before = gauge("configuration_failures")
      intercept[IllegalArgumentException] {
        BoundedTelemetryRuntime.startWith(throw new IllegalArgumentException("bad config"))
      }
      gauge("configuration_failures") shouldBe before + 1L
      val local = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = ()
        override def manifest(value: String): Unit = ()
      }
      val cloud = new BoundedTelemetryQueue.Writer {
        override def record(line: String): Unit    = throw new IllegalStateException("unavailable")
        override def manifest(value: String): Unit = ()
      }
      val conf = new Configuration(false)
      val q = new BoundedTelemetryQueue(
        1,
        100,
        local,
        Some(BoundedTelemetryQueue.Destination("file:/local", conf)),
        Seq(cloud -> BoundedTelemetryQueue.Destination("abfss://fs@account/path", conf))
      )
      BoundedTelemetryRuntime.startWith(Some(q))
      try q.offer("{}") shouldBe true
      finally BoundedTelemetryRuntime.stop(q, 10000) shouldBe true
      gauge("accepted") shouldBe 1L
      gauge("published") shouldBe 0L
      gauge("export_failures") shouldBe 1L
      gauge("local_published") shouldBe 1L
      gauge("abfss_failures") shouldBe 1L
    }
  }
}
