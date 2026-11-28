package org.openivm.spark.common.rocksdb

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.{FileStatus, LocalFileSystem, Path => HadoopPath}
import org.apache.spark.sql.SparkSession
import org.openivm.spark.commands.CommandConcurrencyInjection
import org.openivm.spark.common.{
  CdfWatermarkCatalog,
  ChangeWatermark,
  DeltaTableVersion,
  FeatureGate,
  MvCatalog,
  MvMetadata,
  RefreshTypeCode
}
import org.openivm.spark.perf.{StateGuardPerfTag, StateGuardReport}
import org.openivm.spark.telemetry.BoundedTelemetryRuntime
import org.openivm.spark.testkit.TestPools
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class StateGuardCdfGapFileSystem extends LocalFileSystem {
  private def beforeRead(path: HadoopPath): Unit = {
    val hook = StateGuardCrashProbe.beforeCdfWatermarkWrite
    if (hook != null && path.toString.contains("/cdf_watermarks")) {
      val stack = Thread.currentThread().getStackTrace.toVector
      if (
        stack
          .exists(f => f.getClassName.contains("CdfChangePropagation") && f.getMethodName.contains("markConsumed")) &&
        stack.exists(_.getMethodName.contains("postRefreshCleanup"))
      ) {
        StateGuardCrashProbe.beforeCdfWatermarkWrite = null
        hook()
      }
    }
  }

  override def getFileStatus(path: HadoopPath): FileStatus     = { beforeRead(path); super.getFileStatus(path) }
  override def listStatus(path: HadoopPath): Array[FileStatus] = { beforeRead(path); super.listStatus(path) }
}

/** Child process for [[StateGuardCrashRecoverySpec]]: the `crash` phase kills itself at a state transition, the
  * `recover` phase runs in a fresh JVM against the same warehouse, metastore and state-sync remote.
  */
private[rocksdb] object StateGuardCrashProbe {
  val CrashExit = 73

  private val BeforeCommit                            = "state_sync_before_snapshot_commit"
  private val AfterCommit                             = "state_sync_after_snapshot_commit"
  private val BeforeFinal                             = "refresh_before_finalize"
  private val AckedRefresh                            = "acked_refresh_durable"
  val CdfWatermarkGap                                 = "mv_version_and_cdf_watermark_gap"
  val Points: Seq[String]                             = Seq(BeforeCommit, AfterCommit, BeforeFinal, AckedRefresh)
  @volatile var beforeCdfWatermarkWrite: (() => Unit) = _

  private val source = "default.sgcrash_events"
  private val mv     = "default.sgcrash_mv"
  private val query  = s"SELECT symbol, COUNT(*) AS events, SUM(amount) AS total FROM $source GROUP BY symbol"

  private final class Fixture(spark: SparkSession, root: Path, mode: String) {
    private val mapper = new ObjectMapper()
    val cdf            = mode == "cdf"

    def sql(text: String): Unit   = { spark.sql(text).collect(); () }
    def metadata                  = MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(mv)).get
    def mvVersion(): Long         = DeltaTableVersion.requireLatest(spark, metadata.location)
    def sourceVersion(): Long     = spark.sql(s"DESCRIBE HISTORY $source LIMIT 1").head().getLong(0)
    def watermark(): Option[Long] = CdfWatermarkCatalog.get(spark, mv, metadata.sourceTables.head)

    def requireCdfGap(beforeMv: Long, beforeWatermark: Long, pendingVersion: Long): Unit = {
      val meta = metadata
      require(mvVersion() > beforeMv && converged(), "MV data and catalog advance must commit before the crash")
      require(watermark().contains(beforeWatermark), "the separate CDF watermark advanced before the crash")
      val persisted = meta.properties
        .get(s"${MvMetadata.WatermarkKeyPrefix}${meta.sourceTables.head}")
        .flatMap(ChangeWatermark.decodeDeltaVersion)
        .map(_.version)
      require(persisted.contains(pendingVersion), "MV metadata did not durably record the applied CDF version")
      require(parity(), "MV data diverged before the watermark crash")
    }

    def parity(): Boolean = {
      val actual   = spark.sql(s"SELECT symbol, events, total FROM $mv")
      val expected = spark.sql(query)
      actual.exceptAll(expected).count() == 0L && expected.exceptAll(actual).count() == 0L
    }

    def converged(): Boolean = {
      val meta = metadata
      meta.lastVersion == DeltaTableVersion.requireLatest(spark, meta.location)
    }

    def delta(step: Int): Unit = {
      sql(
        s"INSERT INTO $source SELECT concat('sym_', cast(id % 10 as string)), cast(${step * 1000} + id as int) " +
          "FROM range(20)"
      )
      sql(s"DELETE FROM $source WHERE symbol = 'sym_${step % 10}' AND amount % 2 = 0")
    }

    def refresh(requestId: String): Unit = {
      spark.sparkContext.setLocalProperty("openivm.request_id", requestId)
      try sql(s"REFRESH MATERIALIZED VIEW $mv")
      finally spark.sparkContext.setLocalProperty("openivm.request_id", null)
    }

    def outcome(requestId: String): String = {
      BoundedTelemetryRuntime.awaitDrain(10000L)
      val files = Files.list(root.resolve("phase-events"))
      val nodes =
        try
          files
            .iterator()
            .asScala
            .filter(_.getFileName.toString.endsWith(".jsonl"))
            .flatMap(file => Files.readAllLines(file, UTF_8).asScala.map(line => mapper.readTree(line)))
            .toVector
        finally files.close()
      nodes
        .find(n =>
          n.path("request_id").asText() == requestId && n.path("operation").asText() == "refresh" &&
            n.has("effective_refresh_type")
        )
        .map(_.path("outcome").asText())
        .getOrElse("missing")
    }
  }

  def main(args: Array[String]): Unit = {
    val root    = Paths.get(args(0))
    val phase   = args(1)
    val point   = args(2)
    val backend = args(3)
    val mode    = args(4)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"state-guard-crash-$mode-$phase-$point")
      .config("spark.ui.enabled", "false")
      .config("spark.plugins", "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin")
      .config("spark.openivm.telemetry.queue.enabled", "true")
      .config("spark.openivm.telemetry.queue.localUri", root.resolve("phase-events").toUri.toString)
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$root/metastore;create=true")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config("spark.hadoop.fs.file.impl", classOf[StateGuardCdfGapFileSystem].getName)
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")
      .config(FeatureGate.EnabledKey, "true")
      .config(FeatureGate.ChangeFeedModeKey, mode)
      .config(FeatureGate.QueryLogEnabledKey, (mode == "cdf").toString)
      .config(FeatureGate.CatalogBackendKey, backend)
      .config(FeatureGate.StatePathKey, root.resolve("state").toString)
      .config(FeatureGate.StateSyncUriKey, root.resolve("remote").toUri.toString)
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .config(FeatureGate.GcEnabledKey, "false")
      .getOrCreate()
    try {
      val fixture = new Fixture(spark, root, mode)
      if (phase == "crash") crash(fixture, root, point) else recover(fixture, root, point)
    } finally spark.stop()
  }

  private def die(root: Path, point: String): Unit = {
    Files.write(root.resolve("crash-marker"), point.getBytes(UTF_8))
    Runtime.getRuntime.halt(CrashExit)
  }

  private def crash(f: Fixture, root: Path, point: String): Unit = {
    val props = if (f.cdf) " TBLPROPERTIES ('delta.enableChangeDataFeed'='true')" else ""
    f.sql(s"CREATE TABLE $source(symbol STRING, amount INT) USING DELTA$props")
    f.sql(s"INSERT INTO $source SELECT concat('sym_', cast(id % 10 as string)), cast(id as int) FROM range(100)")
    f.sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
    require(f.metadata.refreshType != RefreshTypeCode.FullRefresh, "fixture MV must be incremental")
    f.delta(1)
    f.refresh("sgcrash-setup")
    require(f.parity(), "fixture MV diverged before the crash")
    Files.write(root.resolve("receipt"), ProcessHandle.current().pid().toString.getBytes(UTF_8))
    f.delta(2)
    point match {
      case BeforeCommit =>
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => die(root, point))
        f.refresh("sgcrash-crash")
      case AfterCommit =>
        OpenIvmStateSync.setAfterSnapshotCommitHookForTesting(() => die(root, point))
        f.refresh("sgcrash-crash")
      case BeforeFinal =>
        CommandConcurrencyInjection.withBeforeRefreshFinalize(die(root, point))(f.refresh("sgcrash-crash"))
      case AckedRefresh =>
        f.refresh("sgcrash-crash")
        require(f.parity(), "acknowledged refresh did not produce a correct MV")
        die(root, point)
      case CdfWatermarkGap =>
        require(f.cdf, "the watermark gap requires CDF mode")
        val beforeMv        = f.mvVersion()
        val beforeWatermark = f.watermark().get
        val pendingVersion  = f.sourceVersion()
        require(pendingVersion > beforeWatermark, "the crash must interrupt a nonempty CDF batch")
        Files.write(
          root.resolve("gap-receipt"),
          Seq(beforeMv.toString, beforeWatermark.toString, pendingVersion.toString).asJava,
          UTF_8
        )
        val interrupt = () => {
          f.requireCdfGap(beforeMv, beforeWatermark, pendingVersion)
          die(root, point)
        }
        beforeCdfWatermarkWrite = interrupt
        OpenIvmRocksDBRegistry.setAfterSlotSelectionHookForTesting { _ =>
          val stack = Thread.currentThread().getStackTrace.toVector
          if (
            stack.exists(f => f.getClassName.contains("RocksDbCdfWatermarkBackend") && f.getMethodName == "putAll") &&
            stack.exists(_.getMethodName.contains("postRefreshCleanup"))
          ) {
            OpenIvmRocksDBRegistry.setAfterSlotSelectionHookForTesting(_ => ())
            interrupt()
          }
        }
        f.refresh("sgcrash-crash")
      case other => sys.error(s"unknown crash point $other")
    }
    sys.error(s"crash point $point did not fire")
  }

  private def recover(f: Fixture, root: Path, point: String): Unit = {
    require(Files.isRegularFile(root.resolve("crash-marker")), "the crash hook was not reached")
    require(
      new String(Files.readAllBytes(root.resolve("receipt")), UTF_8).toLong != ProcessHandle.current().pid(),
      "recovery must run in a fresh process"
    )
    val meta = f.metadata
    require(meta.refreshType != RefreshTypeCode.FullRefresh, "recovered MV lost its incremental refresh type")
    val beforeRecovery = f.mvVersion()
    if (point == CdfWatermarkGap) {
      val saved = Files.readAllLines(root.resolve("gap-receipt"), UTF_8).asScala.map(_.toLong)
      f.requireCdfGap(saved(0), saved(1), saved(2))
    }
    f.refresh("sgcrash-recover-1")
    val restored      = Files.isDirectory(root.resolve("state"))
    val afterRecovery = f.mvVersion()
    val recoveredOk   = f.parity() && f.converged()
    f.refresh("sgcrash-recover-2")
    val retryClean = f.mvVersion() == afterRecovery && f.parity()
    f.delta(3)
    f.refresh("sgcrash-recover-3")
    val outcome     = f.outcome("sgcrash-recover-3")
    val incremental = outcome != "missing" && outcome.nonEmpty && !outcome.contains("full_refresh")
    val finalOk     = f.parity() && f.converged()
    val ackedKept   = !Seq(AckedRefresh, CdfWatermarkGap).contains(point) || afterRecovery == beforeRecovery

    val result = new java.util.LinkedHashMap[String, AnyRef]()
    result.put("crash_pid", java.lang.Long.valueOf(new String(Files.readAllBytes(root.resolve("receipt")), UTF_8)))
    result.put("recovery_pid", java.lang.Long.valueOf(ProcessHandle.current().pid()))
    result.put("mv_version_before_recovery", java.lang.Long.valueOf(beforeRecovery))
    result.put("mv_version_after_recovery", java.lang.Long.valueOf(afterRecovery))
    result.put("parity", java.lang.Boolean.valueOf(recoveredOk && finalOk))
    result.put("metadata_converged", java.lang.Boolean.valueOf(recoveredOk && finalOk))
    result.put("incremental_after_recovery", java.lang.Boolean.valueOf(incremental && retryClean && ackedKept))
    result.put("state_restored", java.lang.Boolean.valueOf(restored))
    result.put("recovery_outcome", outcome)
    Files.write(root.resolve("recover-result.json"), new ObjectMapper().writeValueAsBytes(result))
  }
}

private[rocksdb] object StateGuardCrashRecoverySpec {
  final case class Point(name: String, mode: String, wipeLocalState: Boolean, notes: String)
}

/** Fresh-process recovery of the state transitions the state-guard candidates touch. Each point kills a JVM at the
  * transition, then recovers in a second JVM and checks parity, metadata convergence and that the next batch is still
  * incremental. Points run in parallel. Opt-in via [[StateGuardPerfTag]].
  */
class StateGuardCrashRecoverySpec extends AnyFunSpec with Matchers {
  import StateGuardCrashRecoverySpec.Point

  private val Modes = Seq("intercept", "cdf")

  private val points = Modes.flatMap(mode => basePoints.map(_.copy(mode = mode)))

  private def basePoints = Seq(
    Point(
      "state_sync_before_snapshot_commit",
      "",
      wipeLocalState = true,
      "killed after the candidate snapshot upload and before its commit record; local state wiped, restored from remote"
    ),
    Point(
      "state_sync_after_snapshot_commit",
      "",
      wipeLocalState = true,
      "killed after the snapshot commit and before the refresh returned; local state wiped, restored from remote"
    ),
    Point(
      "refresh_before_finalize",
      "",
      wipeLocalState = false,
      "killed after the MV data apply and before consume, catalog advance and publication; local state kept"
    ),
    Point(
      "acked_refresh_durable",
      "",
      wipeLocalState = true,
      "killed right after a synchronous acknowledgment; local state wiped, the acknowledged refresh must not replay"
    )
  )

  private def launch(root: Path, phase: String, point: Point, backend: String): Option[Int] = {
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val modules = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    val process = new ProcessBuilder(
      (Seq(java, "-Xmx2g") ++ modules ++ Seq(
        "-cp",
        System.getProperty("java.class.path"),
        "org.openivm.spark.common.rocksdb.StateGuardCrashProbe",
        root.toString,
        phase,
        point.name,
        backend,
        point.mode
      )): _*
    ).redirectErrorStream(true).redirectOutput(root.resolve(s"$phase.log").toFile).start()
    try if (process.waitFor(300, TimeUnit.SECONDS)) Some(process.exitValue()) else None
    finally if (process.isAlive) process.destroyForcibly()
  }

  private def deleteTree(path: Path): Unit =
    if (Files.exists(path)) {
      val walk = Files.walk(path)
      try
        walk.sorted(java.util.Comparator.reverseOrder[Path]()).iterator().asScala.toVector.foreach(Files.deleteIfExists)
      finally walk.close()
    }

  private def runPoint(point: Point, backend: String): (Map[String, Any], Path) = {
    val root = Paths.get("target").toAbsolutePath.resolve(s"state-guard-crash-${UUID.randomUUID().toString.take(8)}")
    Files.createDirectories(root)
    val crashExit = launch(root, "crash", point, backend)
    val crashed =
      crashExit.contains(StateGuardCrashProbe.CrashExit) && Files.isRegularFile(root.resolve("crash-marker"))
    var recoverExit: Option[Int] = None
    val result: Option[JsonNode] =
      if (!crashed) None
      else {
        if (point.wipeLocalState) deleteTree(root.resolve("state"))
        recoverExit = launch(root, "recover", point, backend)
        val file = root.resolve("recover-result.json")
        if (recoverExit.contains(0) && Files.isRegularFile(file)) Some(new ObjectMapper().readTree(file.toFile))
        else None
      }
    def flag(name: String): Boolean = result.exists(_.path(name).asBoolean(false))
    val restored                    = !point.wipeLocalState || flag("state_restored")
    val diagnostics =
      if (!crashed) s"; crash phase exit=${crashExit.map(_.toString).getOrElse("timeout")}, logs in $root"
      else if (result.isEmpty)
        s"; recover phase exit=${recoverExit.map(_.toString).getOrElse("timeout")}, logs in $root"
      else if (!restored) "; local state was not restored from the remote"
      else ""
    val report = Map[String, Any](
      "name"                       -> point.name,
      "mode"                       -> point.mode,
      "crashed"                    -> crashed,
      "recovered"                  -> (result.nonEmpty && restored),
      "parity"                     -> flag("parity"),
      "metadata_converged"         -> flag("metadata_converged"),
      "incremental_after_recovery" -> flag("incremental_after_recovery"),
      "known_gap"                  -> (point.name == StateGuardCrashProbe.CdfWatermarkGap),
      "notes"                      -> (point.notes + diagnostics)
    )
    val evidence = StateGuardReport.reportDir.toPath.resolve("crash-probes").resolve(s"${point.mode}-${point.name}")
    Files.createDirectories(evidence)
    Seq("crash.log", "recover.log", "crash-marker", "receipt", "gap-receipt", "recover-result.json").foreach { name =>
      val file = root.resolve(name)
      if (Files.isRegularFile(file)) Files.copy(file, evidence.resolve(name))
    }
    (report, root)
  }

  private val watermarkGap = Point(
    StateGuardCrashProbe.CdfWatermarkGap,
    "cdf",
    wipeLocalState = false,
    "killed after durable MV-version publication and before the separate CDF-watermark write; " +
      "fresh-process recovery must preserve parity and retry without replay; retained as known_gap by the frozen evaluator"
  )

  it("recovers in a fresh process from kills at state-sync and refresh finalization transitions", StateGuardPerfTag) {
    val backend   = StateGuardReport.catalogBackend
    val exercised = points :+ watermarkGap
    val runs = TestPools.withPool(exercised.size) { implicit ec =>
      Await.result(Future.sequence(exercised.map(p => Future(runPoint(p, backend)))), 40.minutes)
    }
    val reports = runs.map(_._1)
    StateGuardReport.write(
      "state-guard-crash.json",
      Map[String, Any](
        "schema"          -> "openivm.state-guard-crash/v1",
        "spark_target"    -> StateGuardReport.sparkTarget,
        "head_sha"        -> StateGuardReport.headSha,
        "catalog_backend" -> backend,
        "points"          -> reports
      )
    )
    val failing = reports.filterNot(r =>
      Seq("crashed", "recovered", "parity", "metadata_converged", "incremental_after_recovery").forall(r(_) == true)
    )
    withClue(failing.map(r => s"${r("mode")}/${r("name")}: ${r("notes")}").mkString("; ")) {
      failing shouldBe empty
    }
    runs.foreach { case (_, root) => deleteTree(root) }
  }
}
