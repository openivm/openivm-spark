package org.openivm.spark.common.rocksdb

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{FeatureGate, MvCatalog, RefreshTypeCode}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

private[rocksdb] object InterceptStaleSnapshotCrashProbe {
  def main(args: Array[String]): Unit = {
    val root                = Paths.get(args(0))
    val phase               = args(1)
    val backend             = args(2)
    val snapshotAfterInsert = args(3).toBoolean
    val mergeChanges        = args.lift(4).contains("merge")
    val source              = "default.barrier_crash_source"
    val mv                  = "default.barrier_crash_mv"
    val query               = s"SELECT id, SUM(amount) AS total FROM $source GROUP BY id"
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(s"intercept-stale-snapshot-crash-$phase")
      .config("spark.ui.enabled", "false")
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
      .config("spark.openivm.changeFeed.mode", "intercept")
      .config(FeatureGate.StatePathKey, root.resolve("state").toString)
      .config(FeatureGate.StateSyncUriKey, root.resolve("remote").toUri.toString)
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .config(FeatureGate.CatalogBackendKey, backend)
      .config(FeatureGate.CatalogPathKey, root.resolve("delta-catalog").toUri.toString)
      .config(FeatureGate.GcEnabledKey, "false")
      .getOrCreate()
    def sql(text: String): Unit = { spark.sql(text).collect(); () }
    def parity(): Unit = {
      val actual   = spark.sql(s"SELECT id, total FROM $mv")
      val expected = spark.sql(query)
      require(
        actual.exceptAll(expected).count() == 0L && expected.exceptAll(actual).count() == 0L,
        "materialized view differs from full recomputation"
      )
      if (mergeChanges)
        require(
          MvCatalog.lookup(spark, spark.sessionState.sqlParser.parseTableIdentifier(mv)).get.refreshType !=
            RefreshTypeCode.FullRefresh,
          "MERGE recovery must stay incremental"
        )
    }
    try {
      if (phase == "crash") {
        sql(s"CREATE TABLE $source(id INT, amount INT) USING DELTA")
        sql(s"INSERT INTO $source VALUES (1, 10)")
        if (mergeChanges) sql(s"INSERT INTO $source VALUES (2, 20)")
        sql(s"CREATE MATERIALIZED VIEW $mv AS $query")
        if (!snapshotAfterInsert) OpenIvmStateSync.backupNow(spark)
        if (mergeChanges)
          sql(s"""
            MERGE INTO $source t
            USING (SELECT * FROM VALUES (1, 25), (2, -1), (3, 50) AS changes(id, amount)) s ON t.id = s.id
            WHEN MATCHED AND s.amount < 0 THEN DELETE
            WHEN MATCHED THEN UPDATE SET t.amount = t.amount + s.amount
            WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
          """)
        else sql(s"INSERT INTO $source VALUES (1, 25)")
        if (snapshotAfterInsert) OpenIvmStateSync.backupNow(spark)
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => {
          parity()
          Files.write(root.resolve("crash-pid"), ProcessHandle.current().pid().toString.getBytes(UTF_8))
          Runtime.getRuntime.halt(73)
        })
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        sys.error("snapshot commit crash hook did not fire")
      } else {
        require(phase == "recover")
        val crashPid = new String(Files.readAllBytes(root.resolve("crash-pid")), UTF_8).toLong
        require(crashPid != ProcessHandle.current().pid(), "recovery must run in another JVM")
        parity()
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        parity()
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        parity()
        sql(s"INSERT INTO $source VALUES (1, 3), (2, 5)")
        sql(s"REFRESH MATERIALIZED VIEW $mv")
        parity()
      }
    } finally spark.stop()
  }
}

class InterceptStaleSnapshotCrashSpec extends AnyFunSpec with Matchers {
  private def deleteTree(path: Path): Unit =
    if (Files.exists(path))
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }
          override def postVisitDirectory(dir: Path, error: java.io.IOException): FileVisitResult = {
            if (error != null) throw error
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )

  private def checkRecovery(backend: String, snapshotAfterInsert: Boolean, mergeChanges: Boolean = false): Unit = {
    val root = Paths.get("target").toAbsolutePath.resolve(s"intercept-stale-snapshot-${UUID.randomUUID()}")
    Files.createDirectories(root)
    val javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val modules = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    def run(phase: String, expected: Int): Unit = {
      val log = root.resolve(s"$phase.log")
      val process = new ProcessBuilder(
        (Seq(javaBin, "-Xmx2g") ++ modules ++ Seq(
          "-cp",
          System.getProperty("java.class.path"),
          "org.openivm.spark.common.rocksdb.InterceptStaleSnapshotCrashProbe",
          root.toString,
          phase,
          backend,
          snapshotAfterInsert.toString,
          if (mergeChanges) "merge" else "insert"
        )): _*
      ).redirectErrorStream(true).redirectOutput(log.toFile).start()
      try {
        withClue(s"$phase: see $log: ") {
          process.waitFor(240, TimeUnit.SECONDS) shouldBe true
          process.exitValue() shouldBe expected
        }
      } finally if (process.isAlive) process.destroyForcibly()
    }
    run("crash", 73)
    val local = root.resolve("state").resolve("_openivm")
    deleteTree(local)
    run("recover", 0)
    if (mergeChanges)
      Option(System.getProperty("openivm.test.log.dir")).foreach { directory =>
        Seq("crash.log", "recover.log", "crash-pid").foreach { name =>
          Files.copy(root.resolve(name), Paths.get(directory).resolve(s"${root.getFileName}-$name"))
        }
      }
    deleteTree(root)
  }

  for (backend <- Seq("delta", "rocksdb"); snapshotAfterInsert <- Seq(false, true)) {
    it(s"recovers $backend after a ${if (snapshotAfterInsert) "post" else "pre"}-input snapshot") {
      checkRecovery(backend, snapshotAfterInsert)
    }
    it(s"recovers $backend MERGE changes after a ${if (snapshotAfterInsert) "post" else "pre"}-input snapshot") {
      checkRecovery(backend, snapshotAfterInsert, mergeChanges = true)
    }
  }
}
