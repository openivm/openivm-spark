package org.openivm.spark.parity

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.util.UUID

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshProfileCatalog, RefreshTypeCode}
import org.openivm.spark.common.rocksdb.OpenIvmRocksDBRegistry
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, BoundedTelemetryRuntime}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.jdk.CollectionConverters._

class PluginCorrelationIntegrationSpec
    extends IvmParitySpecBase("plugin-correlation")
    with InterceptMode
    with Eventually {
  private val suffix = UUID.randomUUID().toString.take(8)
  private val queueDir = new File(s"target/plugin-correlation-queue-$suffix").getAbsoluteFile
  private val syncDir = new File(s"target/plugin-correlation-sync-$suffix").getAbsoluteFile
  private val mapper = new ObjectMapper()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(1, Seconds))

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.plugins" -> "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin",
    BoundedTelemetryQueue.EnabledKey -> "false",
    FeatureGate.StateSyncUriKey -> syncDir.toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey -> "false",
    FeatureGate.StateSyncCheckpointEnabledKey -> "true",
    FeatureGate.ProfileRefreshKey -> "true",
    FeatureGate.CompileClassificationCacheEnabledKey -> "false"
  )

  override def afterAll(): Unit =
    try {
      super.afterAll()
      saveEvidence()
    } finally {
      deleteDir(queueDir)
      deleteDir(syncDir)
    }

  private def eventFiles: Vector[File] =
    Option(queueDir.listFiles()).toVector.flatten.filter(_.getName.endsWith(".jsonl")).sortBy(_.getName)

  private def saveEvidence(): Unit =
    sys.env.get("OPENIVM_PLUGIN_CORRELATION_EVIDENCE").filter(_.nonEmpty).foreach { value =>
      val requested = new File(value)
      require(requested.isAbsolute, "OPENIVM_PLUGIN_CORRELATION_EVIDENCE must be an absolute file path")
      val output = requested.getCanonicalFile.toPath
      require(output.getParent != null && Files.isDirectory(output.getParent), "evidence parent directory must exist")
      Seq(queueDir, syncDir, new File(warehouseDir)).foreach { root =>
        require(!output.startsWith(root.getCanonicalFile.toPath), "evidence must be outside test cleanup directories")
      }
      val health = BoundedTelemetryRuntime.health.getOrElse(fail("plugin queue did not start; no evidence"))
      require(!health.workerAlive, "plugin queue is still draining; evidence is incomplete")
      val files = eventFiles
      require(files.nonEmpty, "plugin queue produced no evidence files")
      val writer = Files.newBufferedWriter(
        output,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
      )
      try files.foreach { file =>
        Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      }
      finally writer.close()
    }

  private def records: Vector[JsonNode] =
    eventFiles.flatMap { file =>
      Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.map(line => mapper.readTree(line))
    }

  private def text(node: JsonNode, field: String): String = node.path(field).asText("")

  private def runRefresh(source: String, view: String, request: String): Seq[org.apache.spark.sql.Row] = {
    sql(s"CREATE TABLE $source(id INT, name STRING, age INT) USING DELTA")
    sql(s"INSERT INTO $source VALUES (1, 'Alice', 30), (2, 'Bob', 28), (3, 'Carol', 35)")
    spark.sparkContext.setLocalProperty("openivm.request_id", s"$request-create")
    sql(s"CREATE MATERIALIZED VIEW $view AS SELECT name, age FROM $source").collect()
    val id = spark.sessionState.sqlParser.parseTableIdentifier(view)
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
    sql(s"DELETE FROM $source WHERE id = 2")
    spark.sparkContext.setLocalProperty("openivm.request_id", request)
    refreshMv(view)
    val outcomes = RefreshProfileCatalog.scanAll(spark).filter(row =>
      row.viewName.split("\\.").last == view && row.stepName == "total_refresh"
    )
    outcomes should have size 1
    outcomes.head.detail should include("outcome=incremental_executed")
    assertMvCorrect(view, s"SELECT name, age FROM $source")
    MvCatalog.lookup(spark, id).get.refreshType shouldBe RefreshTypeCode.SimpleProjection
    spark.table(view).select("name", "age").collect().toSeq.sortBy(_.getString(0))
  }

  it("preserves incremental results while exporting correlated real plugin events") {
    val baseline = runRefresh("plugcorr_off_src", "plugcorr_off_mv", "plugcorr-off")
    BoundedTelemetryRuntime.health shouldBe None
    OpenIvmRocksDBRegistry.closeAll()
    restartSpark(extraSparkConf ++ Map(
      BoundedTelemetryQueue.EnabledKey -> "true",
      BoundedTelemetryQueue.LocalUriKey -> queueDir.toURI.toString,
      BoundedTelemetryQueue.CapacityKey -> "65536"
    ))
    val observed = runRefresh("plugcorr_on_src", "plugcorr_on_mv", "plugcorr-on")
    observed shouldBe baseline

    eventually {
      val events = records
      val phases = events.filter(n => text(n, "event_type") == "phase" && text(n, "request_id") == "plugcorr-on")
      phases should not be empty
      val refreshIds = phases.map(n => text(n, "refresh_id")).filter(_.nonEmpty).distinct
      refreshIds should have size 1
      val refreshId = refreshIds.head
      phases.map(n => text(n, "operation")).distinct shouldBe Vector("refresh")
      phases.map(n => text(n, "phase")) should contain("generate_refresh_sql.compile")
      phases.map(n => text(n, "phase")) should contain("execute_refresh_sql_stmt")
      val barrier = phases.filter(n => Set("state_barrier_wait", "state_barrier_hold").contains(text(n, "phase")))
      barrier.map(n => text(n, "phase")).toSet shouldBe Set("state_barrier_wait", "state_barrier_hold")
      barrier.foreach { node =>
        text(node, "exclusive_scope") shouldBe "unavailable_overlapping_interval"
        node.has("exclusive_ns") shouldBe false
        node.path("inclusive_ns").asLong(-1L) should be >= 0L
      }
      val compiler = events.filter(n => text(n, "record_type") == "compiler_phase" &&
        text(n, "operation_id") == refreshId)
      compiler should not be empty
      compiler.foreach(n => text(n, "request_id") shouldBe "plugcorr-on")
      val createPhases = events.filter(n => text(n, "event_type") == "phase" &&
        text(n, "request_id") == "plugcorr-on-create" && text(n, "operation") == "create")
      createPhases should not be empty
      val createIds = createPhases.map(n => text(n, "refresh_id")).filter(_.nonEmpty).distinct
      createIds should have size 1
      events.exists(n => text(n, "record_type") == "compiler_phase" &&
        text(n, "operation_id") == createIds.head && text(n, "request_id") == "plugcorr-on-create") shouldBe true
      val starts = events.filter(n => text(n, "event_type") == "spark_job_start").filter { node =>
        val action = mapper.readTree(text(node, "action_context"))
        text(action, "action").startsWith(s"$refreshId/") && Set("sql", "collect").contains(text(action, "boundary"))
      }
      starts should not be empty
      val attempts = phases.filter(n => text(n, "phase") == "execute_refresh_sql_attempt")
      attempts should not be empty
      starts.exists { node =>
        val action = text(mapper.readTree(text(node, "action_context")), "action").split("/", -1)
        action.length == 4 && attempts.exists(n => text(n, "detail").contains(s"query_log_order=${action(1)};"))
      } shouldBe true
      val endedIds = events.filter(n => text(n, "event_type") == "spark_job_end").map(_.path("job_id").asInt()).toSet
      starts.exists(n => endedIds.contains(n.path("job_id").asInt())) shouldBe true
      val stageIds = starts.flatMap(_.path("stage_ids").elements().asScala.map(_.asInt())).toSet
      val completedStages = events.filter(n => text(n, "event_type") == "spark_stage_completed")
        .map(_.path("stage_id").asInt()).toSet
      stageIds.intersect(completedStages) should not be empty
      val spans = events.filter(n => text(n, "request_id") == "plugcorr-on" &&
        text(n, "operation") == "refresh" && text(n, "outcome") == "incremental_executed")
      spans should not be empty
      spans.exists(n => text(n, "effective_refresh_type") == "SIMPLE_PROJECTION") shouldBe true
      val requests = spans.flatMap(n => n.path("state_sync").path("requests").elements().asScala)
      requests.exists { n =>
        val acknowledged = text(n, "acknowledged_snapshot_generation_id").nonEmpty
        val publication = n.path("published").asBoolean()
        val reused = n.path("reused").asBoolean() && n.path("coverage_proven").asBoolean()
        text(n, "parent_request_id") == "plugcorr-on" && text(n, "parent_operation_id") == refreshId &&
          text(n, "effective_state_sync_mode") == "synchronous" && text(n, "status") == "success" &&
          acknowledged && (publication || reused)
      } shouldBe true
      val health = BoundedTelemetryRuntime.health.getOrElse(fail("plugin queue not started"))
      health.accepted shouldBe health.published
      health.queued shouldBe 0
      health.droppedFull + health.droppedOversized + health.droppedClosed + health.droppedContended shouldBe 0L
      health.exportFailures + health.workerFailures shouldBe 0L
    }
    stopSpark()
    val finalHealth = BoundedTelemetryRuntime.health.getOrElse(fail("plugin queue health missing"))
    finalHealth.workerAlive shouldBe false
    finalHealth.accepted shouldBe finalHealth.published
    finalHealth.manifestFailures shouldBe 0L
    finalHealth.exportFailures + finalHealth.workerFailures shouldBe 0L
    finalHealth.droppedFull + finalHealth.droppedOversized + finalHealth.droppedClosed +
      finalHealth.droppedContended shouldBe 0L
  }
}
