package org.openivm.spark.common.rocksdb

import java.io.RandomAccessFile
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.openivm.spark.commands.RefreshSqlLog
import org.openivm.spark.common.{CdfWatermarkCatalog, FeatureGate, MvCatalog, RefreshSqlLogRow, RefreshTypeCode}
import org.openivm.spark.telemetry.BoundedTelemetryRuntime
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

private[rocksdb] object CdfFinalizationCrashProbe {
  private val base  = "default.cdf_gap_source"
  private val mv    = "default.cdf_gap_mv"
  private val query = s"SELECT k, COUNT(*) AS n FROM $base GROUP BY k"

  def main(args: Array[String]): Unit = {
    val root         = Paths.get(args(0))
    val phase        = args(1)
    val firstRefresh = args(2).toBoolean
    if (phase == "classpath") {
      Seq(
        "org.openivm.spark.common.rocksdb.CdfFinalizationCrashProbe",
        "org.openivm.spark.commands.RefreshSqlLog",
        "org.openivm.spark.OpenIvmSparkExtensions",
        "org.apache.spark.sql.SparkSession",
        "io.delta.sql.DeltaSparkSessionExtension"
      ).foreach(name => Class.forName(name))
      return
    }
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"cdf-finalization-crash-$phase")
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
      .config(FeatureGate.EnabledKey, "true")
      .config("spark.openivm.changeFeed.mode", "cdf")
      .config(FeatureGate.QueryLogEnabledKey, "true")
      .config(FeatureGate.StatePathKey, root.resolve("state").toString)
      .config(FeatureGate.GcEnabledKey, "false")
      .getOrCreate()

    def sql(text: String): Unit      = { spark.sql(text).collect(); () }
    def version(table: String): Long = spark.sql(s"DESCRIBE HISTORY $table LIMIT 1").head().getLong(0)
    def metadata = MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(mv)).get
    def parity(): Unit = {
      val actual   = spark.sql(s"SELECT k, n FROM $mv")
      val expected = spark.sql(query)
      require(actual.exceptAll(expected).count() == 0L && expected.exceptAll(actual).count() == 0L)
    }
    def activeRefreshLog(): (RefreshSqlLog, Vector[RefreshSqlLogRow]) = {
      val current = RefreshSqlLog.getClass.getDeclaredField("current")
      current.setAccessible(true)
      val log = current.get(RefreshSqlLog).asInstanceOf[ThreadLocal[RefreshSqlLog]].get()
      require(log != null && log.isActive && log.mode == "refresh" && log.viewName == mv)
      val buffer = classOf[RefreshSqlLog].getDeclaredField("buffer")
      buffer.setAccessible(true)
      val rows = buffer.get(log).asInstanceOf[scala.collection.mutable.ArrayBuffer[RefreshSqlLogRow]].toVector
      (log, rows)
    }
    def lines(path: Path): Vector[String] = Files.readAllLines(path, UTF_8).asScala.toVector
    val receipt                           = root.resolve("before-crash")
    val marker                            = root.resolve("in-watermark-put")
    try {
      if (phase == "crash") {
        sql(s"CREATE TABLE $base(k INT) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
        sql(s"INSERT INTO $base VALUES (1), (2)")
        sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
        require(metadata.refreshType != RefreshTypeCode.FullRefresh)
        if (!firstRefresh) {
          sql(s"INSERT INTO $base VALUES (1)")
          sql(s"REFRESH MATERIALIZED VIEW $mv")
        }
        parity()
        val source = metadata.sourceTables match {
          case Seq(single) => single
          case other       => sys.error(s"Expected one source, got $other")
        }
        val beforeOffset = version(base)
        val expectedBefore = if (firstRefresh) None else Some(beforeOffset)
        require(CdfWatermarkCatalog.get(spark, mv, source) == expectedBefore)
        val beforeMv = version(mv)
        require(metadata.lastVersion == beforeMv)
        sql(s"INSERT INTO $base VALUES (1), (1), (3)")
        val pendingOffset = version(base)
        require(pendingOffset > beforeOffset)
        Files.write(
          receipt,
          Seq(
            ProcessHandle.current().pid().toString,
            source,
            beforeOffset.toString,
            pendingOffset.toString,
            beforeMv.toString
          ).asJava,
          UTF_8
        )
        OpenIvmRocksDBRegistry.setAfterSlotSelectionHookForTesting { _ =>
          val stack = Thread.currentThread().getStackTrace.toVector
          def frame(owner: String, method: String): Boolean =
            stack.exists(f => f.getClassName.contains(owner) && f.getMethodName.contains(method))
          if (
            frame("RocksDbCdfWatermarkBackend", "putAll") &&
            frame("CdfChangePropagation", "markConsumed") &&
            stack.exists(_.getMethodName.contains("postRefreshCleanup"))
          ) {
            val (log, rows) = activeRefreshLog()
            require(BoundedTelemetryRuntime.awaitDrain(10000L), "live phase queue did not drain")
            val paths = Files.list(root.resolve("phase-events"))
            val events =
              try {
                val mapper = new ObjectMapper()
                paths
                  .iterator()
                  .asScala
                  .filter(_.getFileName.toString.endsWith(".jsonl"))
                  .map(path => mapper.readTree(path.toFile))
                  .toVector
              } finally paths.close()
            val starts = events.filter(event =>
              event.path("event_type").asText() == "phase_started" &&
                event.path("refresh_id").asText() == log.refreshId &&
                event.path("request_id").asText() == "cdf-crash-live" &&
                event.path("phase").asText() == "finalize_mark_consumed"
            )
            require(starts.size == 1, "the running refresh did not publish its correlated phase start")
            require(
              !events.exists(event =>
                event.path("event_type").asText() == "phase" &&
                  event.path("refresh_id").asText() == log.refreshId &&
                  event.path("phase_id").asLong() == starts.head.path("phase_id").asLong()
              ),
              "the refresh phase ended before the live observation"
            )
            val merge = rows.filter { row =>
              val sql = row.sqlText.toUpperCase(java.util.Locale.ROOT)
              row.refreshId == log.refreshId && row.mode == "refresh" && row.category == "rewritten_stmt" &&
              row.durationMs >= 0L && sql.contains("MERGE INTO") && sql.contains("CDF_GAP_MV")
            }
            val full = rows.filter(_.category == "full_refresh_stmt")
            val witness = Seq(
              s"refresh_id=${log.refreshId}",
              s"incremental_merges=${merge.size}",
              s"full_refresh_statements=${full.size}"
            ) ++
              rows.map(row => s"${row.category} | ${row.stmtKind} | ${row.durationMs} | ${row.sqlText}") ++
              stack.map(_.toString)
            Files.write(marker, witness.asJava, UTF_8)
            Runtime.getRuntime.halt(if (merge.nonEmpty && full.isEmpty) 73 else 74)
          }
        }
        spark.sparkContext.setLocalProperty("openivm.request_id", "cdf-crash-live")
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        sys.error("Expected the CDF finalization crash hook to fire")
      } else {
        require(phase == "recover")
        val saved = lines(receipt)
        require(Files.isRegularFile(marker), "the exact finalization hook was not reached")
        require(saved(0).toLong != ProcessHandle.current().pid())
        val source        = saved(1)
        val oldOffset     = saved(2).toLong
        val pendingOffset = saved(3).toLong
        val oldMv         = saved(4).toLong
        val liveMv        = version(mv)
        require(liveMv > oldMv, "refresh did not commit MV data before the crash")
        require(metadata.refreshType != RefreshTypeCode.FullRefresh)
        require(metadata.lastVersion == liveMv, "catalog advance was not durable after the crash")
        val expectedBefore = if (firstRefresh) None else Some(oldOffset)
        require(CdfWatermarkCatalog.get(spark, mv, source) == expectedBefore, "watermark advanced before the crash")
        parity()
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        parity()
        val expectedAfter = if (firstRefresh) None else Some(pendingOffset)
        require(CdfWatermarkCatalog.get(spark, mv, source) == expectedAfter)
        val afterRetry = version(mv)
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        require(version(mv) == afterRetry, "a no-change retry modified the MV")
        parity()
        sql(s"INSERT INTO $base VALUES (1), (4)")
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        parity()
        require(CdfWatermarkCatalog.get(spark, mv, source).contains(version(base)))
      }
    } finally spark.stop()
  }
}

class CdfFinalizationCrashSpec extends AnyFunSpec with Matchers {
  private def failureFrames(log: Path): String =
    Try {
      val file = new RandomAccessFile(log.toFile, "r")
      try {
        val size  = math.min(file.length(), 65536L).toInt
        val bytes = new Array[Byte](size)
        file.seek(file.length() - size)
        file.readFully(bytes)
        val frame =
          """(?m)^\s*at org\.openivm\.spark\.common\.rocksdb\.CdfFinalizationCrashProbe\$\.(\w[\w$<>]*)\(CdfFinalizationCrashSpec\.scala:(\d+)\)\s*$""".r
        val frames = frame
          .findAllMatchIn(new String(bytes, UTF_8))
          .take(2)
          .map { m =>
            s"${m.group(1)}(CdfFinalizationCrashSpec.scala:${m.group(2)})"
          }
          .toVector
        if (frames.isEmpty) "no probe frame in the last 64 KiB"
        else s"probe frames: ${frames.mkString(", ")}"
      } finally file.close()
    } match {
      case Success(result) => result
      case Failure(error)  => s"probe diagnostics unavailable (${error.getClass.getSimpleName})"
    }

  private def checkRecovery(firstRefresh: Boolean): Unit = {
    val root = Paths.get("target").toAbsolutePath.resolve(s"cdf-finalization-crash-${UUID.randomUUID()}")
    Files.createDirectories(root)
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val modules = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    for ((phase, exit) <- Seq("classpath" -> 0, "crash" -> 73, "recover" -> 0)) {
      val log = root.resolve(s"$phase.log")
      val process = new ProcessBuilder(
        (Seq(java, "-Xmx2g") ++ modules ++ Seq(
          "-cp",
          System.getProperty("java.class.path"),
          "org.openivm.spark.common.rocksdb.CdfFinalizationCrashProbe",
          root.toString,
          phase,
          firstRefresh.toString
        )): _*
      ).redirectErrorStream(true).redirectOutput(log.toFile).start()
      try {
        val finished = process.waitFor(240, TimeUnit.SECONDS)
        val actual   = if (finished) Some(process.exitValue()) else None
        val detail   = if (!finished || !actual.contains(exit)) s"; ${failureFrames(log)}" else ""
        withClue(s"$phase failed; see $log$detail: ") {
          finished shouldBe true
          actual.get shouldBe exit
        }
      } finally if (process.isAlive) process.destroyForcibly()
    }
  }

  it("reuses local CDF state correctly after death between catalog advance and watermark put") {
    checkRecovery(firstRefresh = false)
  }

  it("does not replay the first refresh when its watermark entry was never written") {
    checkRecovery(firstRefresh = true)
  }
}
