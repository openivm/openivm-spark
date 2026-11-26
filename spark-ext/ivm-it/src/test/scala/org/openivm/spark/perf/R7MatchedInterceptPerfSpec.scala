package org.openivm.spark.perf

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{
  DeltaTableVersion,
  FeatureGate,
  MvCatalog,
  QueryLogExport,
  RefreshSqlLogAsyncFlusher,
  StagingCatalog
}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, BoundedTelemetryRuntime}
import org.openivm.spark.testkit.TestPools
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Companion R7 workload: the historical six-MV matched-intercept INSERT, kept as a separately reported regression.
  *
  * This is never the primary score and is never pooled with the CDF walkthrough. Its clock starts once all six
  * workers have been released and ends at the last REFRESH acknowledgement; local relative evidence only.
  */
class R7MatchedInterceptPerfSpec extends IvmParitySpecBase("r7-matched-intercept") with InterceptMode with Eventually {
  private val remote = new File(warehouseDir, "state-sync-remote")
  private val queue  = new File(warehouseDir, "telemetry-queue")
  private val sync   = new File(remote, ".openivm-state-sync")
  private val mapper = new ObjectMapper
  private val source = "r7mi_events"
  private val views  = (1 to 6).map(i => s"r7mi_mv$i")
  private val label  = sys.env.getOrElse(StateGuardReport.RunLabelEnv, "r7-companion")
  private val ids    = views.map(v => s"$label-$v")

  remote.mkdirs()
  queue.mkdirs()

  override protected def sparkMaster: String = "local[8]"
  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone"                   -> "UTC",
    "spark.sql.shuffle.partitions"                 -> "4",
    "spark.plugins"                                -> "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin",
    FeatureGate.CatalogBackendKey                  -> "delta",
    FeatureGate.CatalogPathKey                     -> new File(warehouseDir, "_openivm_catalog").toURI.toString,
    FeatureGate.StatePathKey                       -> new File(warehouseDir, "local-state").getAbsolutePath,
    FeatureGate.ProfileRefreshKey                  -> "true",
    FeatureGate.QueryLogEnabledKey                 -> "true",
    FeatureGate.WindowRunningIncrementalEnabledKey -> "true",
    FeatureGate.StateSyncUriKey                    -> remote.toURI.toString,
    FeatureGate.StateSyncAsyncEnabledKey           -> "false",
    BoundedTelemetryQueue.EnabledKey               -> "true",
    BoundedTelemetryQueue.LocalUriKey              -> queue.toURI.toString,
    BoundedTelemetryQueue.CapacityKey              -> "65536",
    "fs.file.impl"                                 -> classOf[StateGuardLocalFileSystem].getName,
    "fs.file.impl.disable.cache"                   -> "true",
    "fs.AbstractFileSystem.file.impl"              -> classOf[StateGuardLocalAbstractFs].getName
  ) ++ R7WalkthroughCdfPerfSpec.extraTelemetryConf

  private def expected(i: Int): String =
    s"SELECT symbol, COUNT(*) AS events, SUM(amount * $i) AS total FROM $source GROUP BY symbol"

  private def marker(): Option[String] = {
    val files = Option(new File(sync, "commits").listFiles()).toSeq.flatten.filter(_.getName.endsWith(".commit"))
    files.sortBy(_.getName).lastOption.map { file =>
      val generation = file.getName.stripSuffix(".commit")
      val lines      = Files.readAllLines(file.toPath, StandardCharsets.UTF_8)
      val archive    = new File(sync, s"snapshots/$generation/snapshot.zip")
      lines.get(0) shouldBe "OPENIVM_STATE_SNAPSHOT_V2"
      lines.get(1) shouldBe generation
      archive.isFile shouldBe true
      archive.length() shouldBe lines.get(2).toLong
      generation
    }
  }

  private def events: Vector[JsonNode] =
    Option(queue.listFiles()).toVector.flatten.filter(_.getName.endsWith(".jsonl")).flatMap { file =>
      Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.iterator.map(line => mapper.readTree(line))
    }

  it("measures six caller-isolated grouped refreshes for one intercepted three-row append", StateGuardPerfTag) {
    def mv(view: String) = MvCatalog.lookup(spark, TableIdentifier(view)).get
    val mode             = spark.conf.get("spark.openivm.changeFeed.mode")
    mode shouldBe "intercept"
    val sourceUri = new File(warehouseDir, "source-data").toURI.toString
    sql(
      s"CREATE TABLE $source (event_id INT, symbol STRING, amount INT) USING DELTA LOCATION '$sourceUri' " +
        "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
    )
    val cdf = sql(s"SHOW TBLPROPERTIES $source ('delta.enableChangeDataFeed')").select("value").head().getString(0)
    cdf shouldBe "true"
    sql(s"INSERT INTO $source VALUES (1,'AAA',10),(2,'BBB',20),(3,'CCC',30)")
    val seedRows = spark.table(source).count()
    seedRows shouldBe 3L
    views.zipWithIndex.foreach { case (view, idx) =>
      sql(s"CREATE MATERIALIZED VIEW $view AS ${expected(idx + 1)}")
      assertMvCorrect(view, expected(idx + 1))
    }
    val qualified = mv(views.head).sourceTables match {
      case Seq(one) => one
      case extra    => fail(s"Expected one registered source, got $extra")
    }
    val baseVersion = DeltaTableVersion.requireLatest(spark, qualified)
    sql(s"INSERT INTO $source VALUES (101,'AAA',7),(102,'BBB',11),(103,'CCC',13)")
    val finalVersion = DeltaTableVersion.requireLatest(spark, qualified)
    finalVersion shouldBe (baseVersion + 1L)
    def consumerName(view: String): String = {
      val id = mv(view).name
      id.database.fold(id.table)(db => s"$db.${id.table}")
    }
    val pending = views.map { view =>
      val entries = StagingCatalog.collectFor(spark, consumerName(view), Seq(qualified), mv(view).sourceWatermarks)
      entries.map(_.opType) shouldBe Seq("INSERT")
      entries.head
    }
    pending.map(_.stagingPath).distinct.size shouldBe 1
    spark.conf.set(FeatureGate.TelemetryUriKey, new File(warehouseDir, "completed-telemetry").toURI.toString)
    spark.conf.set(FeatureGate.TelemetryCampaignIdKey, label)
    spark.conf.set(FeatureGate.TelemetryCorrelationIdKey, s"correlation-$label")
    spark.conf.set(FeatureGate.TelemetryPhaseKey, "measured")
    val before  = marker()
    val submit  = new Array[Long](6)
    val ack     = new Array[Long](6)
    val ready   = new CountDownLatch(6)
    val gate    = new CountDownLatch(1)
    var start   = 0L
    val latency = StateGuardLatency.modeled(StateGuardReport.latencyScale)
    StateGuardFsInjection.reset()
    try
      TestPools.withPool(6) { implicit ec =>
        val requests = views.indices.map { idx =>
          Future {
            val sc = spark.sparkContext
            sc.setLocalProperty("openivm.request_id", ids(idx))
            sc.setLocalProperty("openivm.node_id", views(idx))
            try {
              ready.countDown()
              require(gate.await(60L, TimeUnit.SECONDS), "admission gate timed out")
              submit(idx) = System.nanoTime()
              QueryLogExport.begin(spark, ids(idx))
              var succeeded = false
              try {
                refreshMv(views(idx))
                succeeded = true
              } finally QueryLogExport.end(spark, ids(idx), succeeded)
              ack(idx) = System.nanoTime()
            } finally {
              sc.setLocalProperty("openivm.request_id", null)
              sc.setLocalProperty("openivm.node_id", null)
            }
          }
        }
        try {
          require(ready.await(60L, TimeUnit.SECONDS), "six workers did not arrive")
          StateGuardFsInjection.configure(latency)
          start = System.nanoTime()
        } finally gate.countDown()
        Await.result(Future.sequence(requests), 15.minutes)
      }
    finally StateGuardFsInjection.disable()
    val fsOps = StateGuardFsInjection.snapshot()
    Seq("catalog", "state_sync").foreach { area =>
      withClue(s"Modeled filesystem was inactive for $area: ") {
        fsOps.iterator.collect { case (key, n) if key.startsWith(s"$area.") => n }.sum should be > 0L
      }
    }
    RefreshSqlLogAsyncFlusher.awaitQuiescence(120000L) shouldBe true
    BoundedTelemetryRuntime.awaitDrain(60000L) shouldBe true
    val after = marker().getOrElse(fail("No committed state generation after REFRESH"))
    before.forall(_ < after) shouldBe true
    val rowsAfter = spark.table(source).count()
    rowsAfter shouldBe 6L
    val sourceId = sql(s"DESCRIBE DETAIL $source").select("id").head().getString(0)
    val ledger   = StagingCatalog.ledger(spark, qualified).getOrElse(fail(s"No source ledger for $qualified"))
    ledger.tableId shouldBe sourceId
    ledger.version shouldBe finalVersion

    def text(node: JsonNode, k: String): String = node.path(k).asText("")
    val records = eventually(timeout(Span(60, Seconds))) {
      val all = events.filter(n => ids.contains(text(n, "request_id")))
      ids.foreach { id =>
        val mine = all.filter(n => text(n, "request_id") == id)
        mine.exists(n => n.has("effective_refresh_type") && n.has("pending_delta_count")) shouldBe true
        mine.exists(n => text(n, "event_type") == "phase" && text(n, "detail") == "phase=register_views") shouldBe true
        mine.exists(n =>
          text(n, "event_type") == "phase" && text(n, "phase") == "execute_refresh_sql_stmt"
        ) shouldBe true
      }
      all
    }
    def interval(node: JsonNode): StateGuardInterval = {
      val s = node.get("start_monotonic_ns").asLong()
      StateGuardInterval(s, s + node.get("inclusive_ns").asLong())
    }
    def at(ns: Long): Double = (ns - start) / 1e6
    val result = views.indices.map { idx =>
      val view   = views(idx)
      val mine   = records.filter(n => text(n, "request_id") == ids(idx))
      val phases = mine.filter(n => text(n, "event_type") == "phase")
      val regs =
        phases.filter(n => text(n, "phase") == "metadata_pre_sql" && text(n, "detail") == "phase=register_views")
      regs.size shouldBe 1
      val reg   = interval(regs.head)
      val stmts = phases.filter(n => text(n, "phase") == "execute_refresh_sql_stmt").map(interval).sortBy(_.startNs)
      val span  = mine.find(n => n.has("effective_refresh_type") && n.has("pending_delta_count")).get
      text(span, "outcome") shouldBe "incremental_executed"
      text(span, "effective_refresh_type") shouldBe "AGGREGATE_GROUP"
      span.get("pending_delta_count").asInt() shouldBe 1
      val versions = span.get("source_versions")
      versions.size() shouldBe 1
      text(versions.get(0), "relation") shouldBe qualified
      versions.get(0).get("start_version").asLong() shouldBe finalVersion
      versions.get(0).get("end_version").asLong() shouldBe finalVersion
      assertMvCorrect(view, expected(idx + 1))
      val metadata = mv(view)
      metadata.sourceTables shouldBe Seq(qualified)
      StagingCatalog.collectFor(spark, consumerName(view), Seq(qualified), metadata.sourceWatermarks) shouldBe empty
      StagingCatalog
        .collectFor(spark, consumerName(view), Seq(qualified))
        .exists(_.stagingPath == pending(idx).stagingPath) shouldBe false
      val capture = mapper.readTree(QueryLogExport.snapshotJson(spark, ids(idx), 100000, 67108864))
      capture.path("capture_complete").asBoolean() shouldBe true
      capture.path("sql_succeeded").asBoolean() shouldBe true
      val actions =
        capture.path("records").elements().asScala.filter(_.path("category").asText() == "register_source_delta").toList
      actions.size shouldBe 1
      actions.head.path("representation_kind").asText() shouldBe "diagnostic"
      val actionMs   = actions.head.get("duration_ms").asLong()
      val diagnostic = actions.head.get("sql_text").asText()
      diagnostic should include(pending(idx).stagingPath.replace("`", "``"))
      QueryLogExport.release(spark, ids(idx))
      val row = Map[String, Any](
        "view"                     -> view,
        "query"                    -> expected(idx + 1),
        "request_id"               -> ids(idx),
        "submitted_ms"             -> at(submit(idx)),
        "ack_ms"                   -> at(ack(idx)),
        "register_start_ms"        -> at(reg.startNs),
        "register_ms"              -> StateGuardReport.ms(reg.lengthNs),
        "register_action_ms"       -> actionMs,
        "register_diagnostic"      -> diagnostic,
        "first_statement_start_ms" -> at(stmts.head.startNs),
        "first_statement_end_ms"   -> at(stmts.head.endNs),
        "statement_count"          -> stmts.size,
        "outcome"                  -> text(span, "outcome"),
        "effective_refresh_type"   -> text(span, "effective_refresh_type"),
        "pending_batches"          -> span.get("pending_delta_count").asInt(),
        "source"                   -> qualified,
        "start_version"            -> finalVersion,
        "end_version"              -> finalVersion,
        "pending_after"            -> 0,
        "bidirectional_except_all" -> true
      )
      (row, reg, StateGuardInterval(stmts.head.startNs, stmts.last.endNs))
    }
    val client = views.indices.map(i => StateGuardInterval(submit(i), ack(i)))
    StateGuardReport.maxConcurrent(client) shouldBe 6
    StateGuardReport.writeLines("r7-matched-intercept-events.jsonl", records.map(n => mapper.writeValueAsString(n)))
    StateGuardReport.write(
      "r7-matched-intercept.json",
      Map[String, Any](
        "schema"                       -> "openivm.r7-matched-intercept/v1",
        "workload"                     -> "companion:matched-intercept-six-mv-insert",
        "role"                         -> "companion",
        "evidence_class"               -> StateGuardReport.EvidenceClass,
        "telemetry_conf"               -> R7WalkthroughCdfPerfSpec.telemetryPins(spark.conf.getAll),
        "label"                        -> label,
        "head"                         -> StateGuardReport.headSha,
        "spark_version"                -> spark.version,
        "target"                       -> StateGuardReport.sparkTarget,
        "master"                       -> spark.sparkContext.master,
        "store_analyzed_plan_for_view" -> spark.conf.get("spark.sql.legacy.storeAnalyzedPlanForView"),
        "sql_on_files"                 -> spark.conf.get("spark.sql.runSQLOnFiles"),
        "mode"                         -> mode,
        "catalog_backend"              -> spark.conf.get(FeatureGate.CatalogBackendKey),
        "async"                        -> spark.conf.get(FeatureGate.StateSyncAsyncEnabledKey),
        "source_cdf_enabled"           -> cdf.toBoolean,
        "pending_stage_paths"          -> pending.map(_.stagingPath),
        "pending_stage_ops"            -> pending.map(_.opType),
        "storage" -> "file:// local Delta; modeled catalog/state-sync I/O only; staging I/O unmodeled; no OneLake or Fabric platform catalog",
        "latency" -> Map(
          "name"             -> latency.name,
          "scale"            -> latency.scale,
          "catalog_read_ms"  -> latency.catalogReadMs,
          "catalog_write_ms" -> latency.catalogWriteMs,
          "state_read_ms"    -> latency.stateReadMs,
          "state_write_ms"   -> latency.stateWriteMs,
          "state_rename_ms"  -> latency.stateRenameMs
        ),
        "fs_ops"                -> fsOps,
        "seed_rows"             -> seedRows,
        "final_rows"            -> rowsAfter,
        "insert_rows"           -> (rowsAfter - seedRows),
        "source_id"             -> sourceId,
        "ledger_id"             -> ledger.tableId,
        "base_version"          -> baseVersion,
        "final_version"         -> finalVersion,
        "ledger_version"        -> ledger.version,
        "marker_before"         -> before,
        "marker_after"          -> after,
        "makespan_ms"           -> at(ack.max),
        "requests_peak"         -> StateGuardReport.maxConcurrent(client),
        "register_peak"         -> StateGuardReport.maxConcurrent(result.map(_._2)),
        "register_overlap_2_ms" -> StateGuardReport.ms(StateGuardReport.overlapNs(result.map(_._2), 2)),
        "register_overlap_6_ms" -> StateGuardReport.ms(StateGuardReport.overlapNs(result.map(_._2), 6)),
        "engine_envelope_peak"  -> StateGuardReport.maxConcurrent(result.map(_._3)),
        "views"                 -> result.map(_._1)
      )
    )
  }
}
