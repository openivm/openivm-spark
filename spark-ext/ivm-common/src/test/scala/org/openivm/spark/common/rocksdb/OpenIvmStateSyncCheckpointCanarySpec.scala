package org.openivm.spark.common.rocksdb

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.openivm.spark.common.{
  FeatureGate,
  OpenIvmGarbageCollector,
  OpenIvmStatePaths,
  RefreshProfileCatalog,
  RefreshProfileRow
}
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, BoundedTelemetryRuntime, OpenIvmExecutionSpan}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.{CountDownLatch, CopyOnWriteArrayList, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.io.Source

object OpenIvmStateSyncFreshRestoreProbe {
  def main(args: Array[String]): Unit = {
    val warehouse = new File(args(0))
    val uri       = args(1)
    val deleted   = args(2)
    val retained  = args(3)
    val refreshId = args(4)
    val evidence  = new File(args(5))
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("openivm-checkpoint-canary-fresh-restore")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, uri)
      .getOrCreate()
    try {
      OpenIvmStateSync.maybeRestore(spark)
      val root = OpenIvmStatePaths.openIvmRoot(spark)
      require(!Files.exists(root.resolve(deleted)), s"deleted trigger was restored: $deleted")
      require(Files.exists(root.resolve(retained)), s"unrelated trigger was lost: $retained")
      val rows = RefreshProfileCatalog.scanAll(spark)
      require(rows.exists(_.refreshId == refreshId), s"profile-only row $refreshId was not restored")
      Files.createDirectories(evidence.toPath.getParent)
      Files.write(
        evidence.toPath,
        s"""{"fresh_jvm":true,"deleted_absent":true,"retained_present":true,"profile_row":"$refreshId"}"""
          .getBytes(
            StandardCharsets.UTF_8
          )
      )
    } finally {
      OpenIvmRocksDBRegistry.closeAll()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}

class OpenIvmStateSyncCheckpointCanarySpec extends AnyFunSpec with BeforeAndAfterEach with Matchers {
  private val dirs     = scala.collection.mutable.ArrayBuffer.empty[File]
  private val sparks   = scala.collection.mutable.ArrayBuffer.empty[SparkSession]
  private val receipts = new CopyOnWriteArrayList[OpenIvmStateSync.StateSyncRequestReceipt]()

  override def beforeEach(): Unit = {
    OpenIvmStateSync.resetForTesting()
    OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting(_ => ())
    receipts.clear()
    OpenIvmStateSync.setStateSyncRequestReceiptHookForTesting(receipt => { receipts.add(receipt); () })
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting(_ => ())
    OpenIvmRocksDBRegistry.closeAll()
    sparks.reverse.foreach { spark =>
      try spark.stop()
      catch { case _: Throwable => () }
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    OpenIvmStateSync.resetForTesting()
    sparks.clear()
    dirs.reverse.foreach(deleteRecursively)
    dirs.clear()
    super.afterEach()
  }

  private def newDir(prefix: String): File = {
    val dir = new File(s"target/test-checkpoint-canary-$prefix-${UUID.randomUUID().toString.take(8)}")
    dir.mkdirs()
    dirs += dir
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }

  private def newSpark(appName: String, uri: File, extra: Seq[(String, String)] = Seq.empty): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val warehouse = newDir(s"$appName-warehouse")
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, uri.toURI.toString)
      .config(FeatureGate.StateSyncCheckpointEnabledKey, "true")
      .config(FeatureGate.StateSyncAsyncEnabledKey, "false")
      .config(FeatureGate.GcOrphanGracePeriodKey, "0ms")
    extra.foreach { case (key, value) => builder.config(key, value) }
    val spark = builder.getOrCreate()
    sparks += spark
    spark
  }

  private def stopSpark(spark: SparkSession): Unit = {
    OpenIvmRocksDBRegistry.closeAll()
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparks -= spark
  }

  private def recordProfile(spark: SparkSession, refreshId: String): Unit = {
    RefreshProfileCatalog.ensureTables(spark)
    RefreshProfileCatalog.record(
      spark,
      Seq(
        RefreshProfileRow(
          refreshId = refreshId,
          viewName = "canary.profile_only",
          profileTimestamp = Timestamp.valueOf("2025-01-01 00:00:00"),
          stepOrder = 0,
          stepName = "profile_only_fixture",
          durationMs = 7L,
          detail = s"operation_id=$refreshId"
        )
      )
    )
  }

  private def setRequestId(spark: SparkSession, requestId: String): Unit =
    spark.sparkContext.setLocalProperty("openivm.request_id", requestId)

  private def committedGenerations(spark: SparkSession, uri: String): Seq[String] = {
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(spark.sessionState.newHadoopConf())
    val commits    = new Path(new Path(remoteRoot, ".openivm-state-sync"), "commits")
    fs.listStatus(commits)
      .filter(status => status.isFile && status.getPath.getName.endsWith(".commit"))
      .map(_.getPath.getName.stripSuffix(".commit"))
      .sorted
      .toSeq
  }

  private def commitDetailsFor(spark: SparkSession, uri: String, generation: String): (String, String, Long, String) = {
    val remoteRoot = new Path(uri)
    val fs         = remoteRoot.getFileSystem(spark.sessionState.newHadoopConf())
    val commits    = new Path(new Path(remoteRoot, ".openivm-state-sync"), "commits")
    val commit     = new Path(commits, s"$generation.commit")
    fs.exists(commit) shouldBe true
    val input = fs.open(commit)
    val text =
      try Source.fromInputStream(input, "UTF-8").mkString
      finally input.close()
    val lines = text.split("\\n", -1)
    lines.length shouldBe 5
    lines(1) shouldBe generation
    val archive =
      new Path(new Path(new Path(new Path(remoteRoot, ".openivm-state-sync"), "snapshots"), generation), "snapshot.zip")
    (generation, lines(3), lines(2).toLong, sha256(fs, archive))
  }

  private def commitDetails(spark: SparkSession, uri: String): (String, String, Long, String) =
    commitDetailsFor(spark, uri, committedGenerations(spark, uri).last)

  private def awaitSynchronousRequestCount(spark: SparkSession, expected: Long): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (OpenIvmStateSync.synchronousRequestCountForTesting(spark) != expected && System.nanoTime() < deadline)
      Thread.sleep(10L)
    OpenIvmStateSync.synchronousRequestCountForTesting(spark) shouldBe expected
  }

  private def awaitDestinationQueue(spark: SparkSession): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (OpenIvmStateSync.destinationQueueLengthForTesting(spark) < 1 && System.nanoTime() < deadline)
      Thread.sleep(10L)
    OpenIvmStateSync.destinationQueueLengthForTesting(spark) should be >= 1
  }

  private def awaitBackupIdle(): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    while (!OpenIvmStateSync.allBackupStatesIdleForTesting && System.nanoTime() < deadline) Thread.sleep(10L)
    OpenIvmStateSync.allBackupStatesIdleForTesting shouldBe true
  }

  private def awaitNewCommit(
      spark: SparkSession,
      uri: String,
      previousGeneration: String
  ): (String, String, Long, String) = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    var latest   = committedGenerations(spark, uri).lastOption
    while (latest.contains(previousGeneration) && System.nanoTime() < deadline) {
      Thread.sleep(10L)
      latest = committedGenerations(spark, uri).lastOption
    }
    latest.filter(_ != previousGeneration) should not be empty
    commitDetailsFor(spark, uri, latest.get)
  }

  private def sha256(fs: org.apache.hadoop.fs.FileSystem, path: Path): String = {
    val input  = fs.open(path)
    val digest = MessageDigest.getInstance("SHA-256")
    try {
      val bytes = new Array[Byte](8192)
      var count = input.read(bytes)
      while (count >= 0) {
        if (count > 0) digest.update(bytes, 0, count)
        count = input.read(bytes)
      }
    } finally input.close()
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
  }

  private def repoRoot: File = {
    var current = new File(".").getCanonicalFile
    while (current.getParentFile != null && !new File(current, ".git").exists()) current = current.getParentFile
    current
  }

  private def writeReceiptEvidence(
      fixture: String,
      expectedIds: Seq[String],
      latest: (String, String, Long, String),
      extra: String
  ): Unit = {
    val receiptJson = receipts.asScala
      .map { receipt =>
        s"""{"request_id":"${receipt.requestId}","requested_generation":${receipt.requestedMutationGeneration},""" +
          s""""captured_generation":${receipt.capturedMutationGeneration.map(_.toString).getOrElse("null")},""" +
          s""""publication_generation":"${receipt.publicationGeneration.getOrElse("")}",""" +
          s""""published":${receipt.published},"reused":${receipt.reused},""" +
          s""""archive_sha256":"${receipt.archiveSha256.getOrElse("")}",""" +
          s""""archive_bytes":${receipt.archiveBytes.getOrElse(0L)},""" +
          s""""coverage_proven":${receipt.coverageProven},"acknowledged":${receipt.acknowledged},""" +
          s""""outcome":"${receipt.outcome}"}"""
      }
      .mkString("[", ",", "]")
    val output = new File(repoRoot, s".logs/checkpoint-only-canary/$fixture.json")
    Files.createDirectories(output.toPath.getParent)
    val json =
      s"""{"fixture":"$fixture","request_ids":[${expectedIds.map(id => '"'.toString + id + '"').mkString(",")}]""" +
        s""","latest_generation":"${latest._1}","manifest_sha256":"${latest._2}",""" +
        s""""archive_bytes":${latest._3},"archive_sha256":"${latest._4}","receipts":$receiptJson,$extra}"""
    Files.write(output.toPath, json.getBytes(StandardCharsets.UTF_8))
  }

  private def runFreshRestoreProbe(
      uri: String,
      deletedRelative: String,
      retainedRelative: String,
      refreshId: String,
      evidenceFile: File
  ): Unit = {
    val javaHome  = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val classpath = System.getProperty("java.class.path")
    val jvmArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(arg => arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
    val childWarehouse = newDir("fresh-child-warehouse")
    val logFile        = new File(childWarehouse, "fresh-restore.log")
    val process = new ProcessBuilder(
      (Seq(javaHome) ++ jvmArgs ++ Seq(
        "-cp",
        classpath,
        "org.openivm.spark.common.rocksdb.OpenIvmStateSyncFreshRestoreProbe",
        childWarehouse.getAbsolutePath,
        uri,
        deletedRelative,
        retainedRelative,
        refreshId,
        evidenceFile.getAbsolutePath
      )): _*
    )
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
      .start()
    process.waitFor(180, TimeUnit.SECONDS) shouldBe true
    if (process.exitValue() != 0) {
      val input = Source.fromFile(logFile)
      val output =
        try input.mkString
        finally input.close()
      fail(s"Fresh restore JVM exited ${process.exitValue()}:\n$output")
    }
    evidenceFile.isFile shouldBe true
  }

  describe("synchronous checkpoint canary") {
    it("publishes a post-cleanup archive after partial trigger deletion and restores it in a fresh JVM") {
      val fixtureRoot = newDir("partial-sweep")
      val remote      = new File(fixtureRoot, "remote")
      remote.mkdirs()
      val spark     = newSpark("checkpoint-partial-sweep", remote)
      val warehouse = OpenIvmStatePaths.warehouseRoot(spark).toFile
      val refreshId = s"profile-only-${UUID.randomUUID()}"
      recordProfile(spark, refreshId)

      val triggerRoot     = new File(warehouse, "_openivm/triggers")
      val firstTrigger    = new File(triggerRoot, "db-a/operation-a/marker")
      val secondTrigger   = new File(triggerRoot, "db-b/operation-b/marker")
      val currentVersions = org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion)
      currentVersions shouldBe empty
      firstTrigger.getParentFile.mkdirs()
      secondTrigger.getParentFile.mkdirs()
      Files.write(firstTrigger.toPath, Array[Byte](1, 2, 3))
      Files.write(secondTrigger.toPath, Array[Byte](4, 5, 6))
      firstTrigger.setLastModified(1L)
      firstTrigger.getParentFile.setLastModified(1L)
      secondTrigger.setLastModified(1L)
      secondTrigger.getParentFile.setLastModified(1L)

      val cleanupMarker = new File(triggerRoot, "command-cleanup/operation/marker")
      cleanupMarker.getParentFile.mkdirs()
      Files.write(cleanupMarker.toPath, Array[Byte](9))
      cleanupMarker.setLastModified(System.currentTimeMillis() + 60000L)
      var cleanupCallbackRan = false
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() =>
        throw new java.io.IOException("injected commit failure")
      )
      setRequestId(spark, "failed-before-cleanup")
      intercept[java.io.IOException] {
        OpenIvmStateSync.backupForCommand(
          spark,
          () => {
            cleanupCallbackRan = true
            Files.deleteIfExists(cleanupMarker.toPath)
            ()
          }
        )
      }
      cleanupCallbackRan shouldBe false
      cleanupMarker.isFile shouldBe true
      OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)

      setRequestId(spark, "before-partial-cleanup")
      OpenIvmStateSync.backupNow(spark)
      val initial = receipts.asScala.find(_.requestId == "before-partial-cleanup").get
      initial.published shouldBe true
      initial.acknowledged shouldBe true
      initial.reused shouldBe false

      val deletedPath = new java.util.concurrent.atomic.AtomicReference[String]()
      OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting { path =>
        if (deletedPath.compareAndSet(null, path.toString)) throw new java.io.IOException("injected after delete")
      }
      OpenIvmGarbageCollector.sweepNow(
        spark,
        OpenIvmGarbageCollector.Plan(Set.empty, Set.empty),
        System.currentTimeMillis() + 1L
      )
      deletedPath.get() should not be null
      val deletedDirectory = new Path(deletedPath.get()).toUri.getPath
      val deletedFile      = new File(deletedDirectory, "marker")
      deletedFile.isFile shouldBe false
      val remaining = Seq(firstTrigger, secondTrigger).find(file =>
        file.getParentFile.getAbsolutePath != new File(deletedDirectory).getAbsolutePath
      )
      remaining.isDefined shouldBe true
      remaining.get.isFile shouldBe true

      OpenIvmGarbageCollector.setAfterSweepDeleteHookForTesting(_ => ())
      setRequestId(spark, "after-partial-cleanup")
      OpenIvmStateSync.backupNow(spark)
      val afterCleanup = receipts.asScala.find(_.requestId == "after-partial-cleanup").get
      afterCleanup.published shouldBe true
      afterCleanup.acknowledged shouldBe true
      afterCleanup.reused shouldBe false
      afterCleanup.publicationGeneration should not be initial.publicationGeneration
      afterCleanup.coverageProven shouldBe false

      val latest = commitDetails(spark, remote.toURI.toString)
      latest._1 shouldBe afterCleanup.publicationGeneration.get
      latest._2 shouldBe latest._4
      latest._3 should be > 0L
      afterCleanup.archiveSha256 shouldBe Some(latest._2)
      afterCleanup.archiveBytes shouldBe Some(latest._3)
      val childEvidence = new File(repoRoot, ".logs/checkpoint-only-canary/partial-trigger-cleanup-fresh-jvm.json")
      val deletedRelative =
        new File(warehouse, "_openivm/triggers").toPath.relativize(new File(deletedDirectory).toPath)
      val retainedRelative =
        new File(warehouse, "_openivm/triggers").toPath.relativize(remaining.get.getParentFile.toPath)
      val deletedRel  = s"triggers/${deletedRelative.toString}/marker"
      val retainedRel = s"triggers/${retainedRelative.toString}/marker"
      org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion) shouldBe currentVersions
      stopSpark(spark)
      runFreshRestoreProbe(remote.toURI.toString, deletedRel, retainedRel, refreshId, childEvidence)

      writeReceiptEvidence(
        "partial-trigger-cleanup-spark-4.1",
        Seq("before-partial-cleanup", "after-partial-cleanup"),
        latest,
        s""""deleted_trigger_absent_in_fresh_jvm":true,"unrelated_profile_row_preserved":true,""" +
          s""""mv_versions_unchanged":true,"partial_cleanup_failure_injected":true,""" +
          s""""precommit_cleanup_callback_suppressed":${!cleanupCallbackRan},""" +
          s""""fresh_jvm_evidence":"${childEvidence.getAbsolutePath}""""
      )
    }

    it("defers a profile-only mutation after capture until its exact follow-up archive commits") {
      implicit val executionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
      val fixtureRoot                                                  = newDir("late-profile")
      val remote                                                       = new File(fixtureRoot, "remote")
      val stateRoot                                                    = new File(fixtureRoot, "state")
      remote.mkdirs()
      stateRoot.mkdirs()
      val spark = newSpark(
        "checkpoint-late-profile",
        remote,
        Seq(FeatureGate.StatePathKey -> stateRoot.getAbsolutePath)
      )
      OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true
      val ivmRoot  = new File(stateRoot, "_openivm")
      val retained = new File(ivmRoot, "triggers/late-profile/retained/marker")
      retained.getParentFile.mkdirs()
      Files.write(retained.toPath, Array[Byte](7, 8, 9))
      val versionsBefore = org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion)
      versionsBefore shouldBe empty
      recordProfile(spark, s"profile-before-${UUID.randomUUID()}")

      def request(requestId: String): Future[Unit] = Future {
        setRequestId(spark, requestId)
        try OpenIvmStateSync.backupNow(spark)
        finally setRequestId(spark, null)
      }

      Await.result(request("late-profile-baseline"), 60.seconds)
      val baseline = receipts.asScala.find(_.requestId == "late-profile-baseline").get
      baseline.acknowledged shouldBe true
      baseline.archiveSha256 should not be empty

      recordProfile(spark, s"profile-before-capture-${UUID.randomUUID()}")
      val captureStarted       = new CountDownLatch(1)
      val releaseFirstPack     = new CountDownLatch(1)
      val secondCaptureStarted = new CountDownLatch(1)
      val releaseSecondCapture = new CountDownLatch(1)
      val holdPack             = new AtomicBoolean(false)
      val captureNumber        = new AtomicInteger(0)
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting { _ =>
        if (holdPack.compareAndSet(false, true)) {
          captureStarted.countDown()
          releaseFirstPack.await(30, TimeUnit.SECONDS) shouldBe true
        }
      }
      OpenIvmStateSync.setBeforeCaptureHookForTesting { () =>
        if (captureNumber.incrementAndGet() == 2) {
          secondCaptureStarted.countDown()
          releaseSecondCapture.await(30, TimeUnit.SECONDS) shouldBe true
        }
      }
      def awaitRequestCount(expected: Long): Unit = {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (OpenIvmStateSync.synchronousRequestCountForTesting(spark) != expected && System.nanoTime() < deadline)
          Thread.sleep(25L)
        OpenIvmStateSync.synchronousRequestCountForTesting(spark) shouldBe expected
      }
      val countBeforeCapture  = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
      val firstCaptureRequest = request("capture-before-late-profile")
      awaitRequestCount(countBeforeCapture + 1)
      captureStarted.await(10, TimeUnit.SECONDS) shouldBe true
      val capturedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)

      val lateProfileId = s"profile-late-${UUID.randomUUID()}"
      recordProfile(spark, lateProfileId)
      val lateRequestedGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)
      lateRequestedGeneration should be > capturedGeneration
      val lateRequest = request("late-profile-during-capture")
      awaitRequestCount(countBeforeCapture + 2)
      lateRequest.isCompleted shouldBe false

      releaseFirstPack.countDown()
      Await.result(firstCaptureRequest, 60.seconds)
      secondCaptureStarted.await(10, TimeUnit.SECONDS) shouldBe true
      val olderCommitDidNotAckLateRequest = !lateRequest.isCompleted
      olderCommitDidNotAckLateRequest shouldBe true
      val firstPublication = receipts.asScala.find(_.requestId == "capture-before-late-profile").get
      firstPublication.acknowledged shouldBe true
      firstPublication.capturedMutationGeneration shouldBe Some(capturedGeneration)
      firstPublication.capturedMutationGeneration.get should be < lateRequestedGeneration
      val firstCommit = commitDetails(spark, remote.toURI.toString)
      firstCommit._1 shouldBe firstPublication.publicationGeneration.get
      firstPublication.archiveSha256 shouldBe Some(firstCommit._2)
      firstPublication.archiveBytes shouldBe Some(firstCommit._3)

      releaseSecondCapture.countDown()
      Await.result(lateRequest, 60.seconds)
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting(null)
      OpenIvmStateSync.setBeforeCaptureHookForTesting(null)

      val latePublication =
        receipts.asScala.find(receipt => receipt.requestId == "late-profile-during-capture" && receipt.acknowledged).get
      latePublication.published shouldBe true
      latePublication.reused shouldBe false
      latePublication.coverageProven shouldBe true
      latePublication.requestedMutationGeneration shouldBe lateRequestedGeneration
      latePublication.capturedMutationGeneration shouldBe Some(lateRequestedGeneration)
      latePublication.publicationGeneration should not be firstPublication.publicationGeneration
      val latest = commitDetails(spark, remote.toURI.toString)
      latest._1 shouldBe latePublication.publicationGeneration.get
      latePublication.archiveSha256 shouldBe Some(latest._2)
      latePublication.archiveBytes shouldBe Some(latest._3)
      latest._2 should not be firstCommit._2
      org.openivm.spark.common.MvCatalog.list(spark).map(_.lastVersion) shouldBe versionsBefore

      val childEvidence = new File(repoRoot, ".logs/checkpoint-only-canary/late-profile-fresh-jvm.json")
      val missingRel    = "triggers/late-profile/missing/marker"
      val retainedRel   = "triggers/late-profile/retained/marker"
      stopSpark(spark)
      runFreshRestoreProbe(remote.toURI.toString, missingRel, retainedRel, lateProfileId, childEvidence)
      writeReceiptEvidence(
        "late-profile-inflight-spark-4.1",
        Seq("late-profile-baseline", "capture-before-late-profile", "late-profile-during-capture"),
        latest,
        s""""captured_generation_before_late_write":$capturedGeneration,""" +
          s""""late_requested_generation":$lateRequestedGeneration,""" +
          s""""older_commit_did_not_ack_late_request":$olderCommitDidNotAckLateRequest,""" +
          s""""late_archive_differs_from_older_commit":true,""" +
          s""""mv_versions_unchanged":true,"fresh_jvm_profile_restored":true,""" +
          s""""fresh_jvm_evidence":"${childEvidence.getAbsolutePath}""""
      )
    }

    it("holds a committed candidate across queued async commits and rejects its pruned generation") {
      implicit val executionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
      val fixtureRoot                                                  = newDir("candidate-publication-lock")
      val remote                                                       = new File(fixtureRoot, "remote")
      val stateRoot                                                    = new File(fixtureRoot, "state")
      remote.mkdirs()
      stateRoot.mkdirs()
      val spark = newSpark(
        "checkpoint-candidate-publication-lock",
        remote,
        Seq(
          FeatureGate.StatePathKey                  -> stateRoot.getAbsolutePath,
          FeatureGate.StateSyncRetainGenerationsKey -> "2"
        )
      )
      OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true
      val marker = new File(stateRoot, "_openivm/triggers/candidate-lock/retained/marker")
      marker.getParentFile.mkdirs()
      Files.write(marker.toPath, Array[Byte](1, 3, 5, 7))
      recordProfile(spark, s"candidate-lock-${UUID.randomUUID()}")

      val candidatePublished  = new CountDownLatch(1)
      val releaseCandidate    = new CountDownLatch(1)
      val asyncCaptureStarted = new CountDownLatch(1)
      val watchAsyncCapture   = new AtomicBoolean(false)
      OpenIvmStateSync.setBeforeCaptureHookForTesting(() => {
        if (watchAsyncCapture.get()) asyncCaptureStarted.countDown()
      })
      OpenIvmStateSync.setAfterCandidatePublicationHookForTesting(() => {
        candidatePublished.countDown()
        releaseCandidate.await(30, TimeUnit.SECONDS) shouldBe true
      })
      def request(requestId: String): Future[Unit] = Future {
        setRequestId(spark, requestId)
        try OpenIvmStateSync.backupNow(spark)
        finally setRequestId(spark, null)
      }

      var candidateRequest: Future[Unit] = null
      try {
        val requestsBefore = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
        candidateRequest = request("candidate-publication-lock")
        awaitSynchronousRequestCount(spark, requestsBefore + 1)
        candidatePublished.await(30, TimeUnit.SECONDS) shouldBe true

        val candidateCommit = commitDetails(spark, remote.toURI.toString)
        committedGenerations(spark, remote.toURI.toString) shouldBe Seq(candidateCommit._1)
        watchAsyncCapture.set(true)
        OpenIvmStateSync.backupAsync(spark)
        awaitDestinationQueue(spark)
        asyncCaptureStarted.getCount shouldBe 1L
        committedGenerations(spark, remote.toURI.toString) shouldBe Seq(candidateCommit._1)

        releaseCandidate.countDown()
        Await.result(candidateRequest, 60.seconds)
        asyncCaptureStarted.await(30, TimeUnit.SECONDS) shouldBe true
        val candidateReceipt = receipts.asScala.find(_.requestId == "candidate-publication-lock").get
        candidateReceipt.acknowledged shouldBe true
        candidateReceipt.published shouldBe true
        candidateReceipt.reused shouldBe false
        candidateReceipt.publicationGeneration shouldBe Some(candidateCommit._1)
        candidateReceipt.archiveSha256 shouldBe Some(candidateCommit._2)
        candidateReceipt.archiveBytes shouldBe Some(candidateCommit._3)
        commitDetailsFor(spark, remote.toURI.toString, candidateCommit._1) shouldBe candidateCommit

        val firstAsyncCommit = awaitNewCommit(spark, remote.toURI.toString, candidateCommit._1)
        awaitBackupIdle()
        committedGenerations(spark, remote.toURI.toString).toSet shouldBe Set(candidateCommit._1, firstAsyncCommit._1)
        OpenIvmStateSync.backupAsync(spark)
        val secondAsyncCommit = awaitNewCommit(spark, remote.toURI.toString, firstAsyncCommit._1)
        awaitBackupIdle()
        secondAsyncCommit._1 should not be firstAsyncCommit._1
        committedGenerations(spark, remote.toURI.toString) shouldBe Seq(firstAsyncCommit._1, secondAsyncCommit._1)
        committedGenerations(spark, remote.toURI.toString) should not contain candidateCommit._1

        Await.result(request("sync-after-candidate-pruned"), 60.seconds)
        val syncReceipt = receipts.asScala.find(_.requestId == "sync-after-candidate-pruned").get
        val syncCommit  = commitDetails(spark, remote.toURI.toString)
        syncReceipt.acknowledged shouldBe true
        syncReceipt.published shouldBe true
        syncReceipt.reused shouldBe false
        syncReceipt.coverageProven shouldBe true
        syncReceipt.publicationGeneration shouldBe Some(syncCommit._1)
        syncReceipt.publicationGeneration should not be candidateReceipt.publicationGeneration
        syncReceipt.archiveSha256 shouldBe Some(syncCommit._2)
        syncReceipt.archiveBytes shouldBe Some(syncCommit._3)
        commitDetailsFor(spark, remote.toURI.toString, syncReceipt.publicationGeneration.get) shouldBe syncCommit
      } finally {
        releaseCandidate.countDown()
        OpenIvmStateSync.setAfterCandidatePublicationHookForTesting(null)
        OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
        if (candidateRequest != null)
          try Await.ready(candidateRequest, 60.seconds)
          catch { case _: Throwable => () }
        awaitBackupIdle()
      }
    }

    it("cancels a queued candidate waiter without acknowledging it after the active commit") {
      implicit val executionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
      val fixtureRoot                                                  = newDir("cancelled-candidate-waiter")
      val remote                                                       = new File(fixtureRoot, "remote")
      val stateRoot                                                    = new File(fixtureRoot, "state")
      remote.mkdirs()
      stateRoot.mkdirs()
      val spark = newSpark(
        "checkpoint-cancelled-candidate-waiter",
        remote,
        Seq(
          FeatureGate.StatePathKey      -> stateRoot.getAbsolutePath,
          FeatureGate.ProfileRefreshKey -> "true"
        )
      )
      OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true
      val marker = new File(stateRoot, "_openivm/triggers/cancelled-waiter/retained/marker")
      marker.getParentFile.mkdirs()
      Files.write(marker.toPath, Array[Byte](2, 4, 6, 8))
      recordProfile(spark, s"cancelled-waiter-${UUID.randomUUID()}")

      val captureStarted = new CountDownLatch(1)
      val releaseCapture = new CountDownLatch(1)
      OpenIvmStateSync.setBeforeCaptureHookForTesting(() => {
        captureStarted.countDown()
        releaseCapture.await(30, TimeUnit.SECONDS) shouldBe true
      })
      def request(requestId: String): Future[Unit] = Future {
        setRequestId(spark, requestId)
        try OpenIvmStateSync.backupNow(spark)
        finally setRequestId(spark, null)
      }

      val evidence = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => { evidence.add(value); () })
      var activeRequest: Future[Unit] = null
      var waiterThread: Thread        = null
      try {
        val requestsBefore = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
        activeRequest = request("active-candidate-capture")
        awaitSynchronousRequestCount(spark, requestsBefore + 1)
        captureStarted.await(30, TimeUnit.SECONDS) shouldBe true

        val waiterFailure     = new AtomicReference[Throwable]()
        val waiterInterrupted = new AtomicBoolean(false)
        val waiterStarted     = new CountDownLatch(1)
        waiterThread = new Thread(
          new Runnable {
            override def run(): Unit = {
              waiterStarted.countDown()
              setRequestId(spark, "cancelled-candidate-waiter")
              try {
                OpenIvmStateSync.backupNow(spark)
                waiterFailure.set(new AssertionError("cancelled waiter unexpectedly returned successfully"))
              } catch {
                case error: Throwable => waiterFailure.set(error)
              } finally {
                waiterInterrupted.set(Thread.currentThread().isInterrupted)
                setRequestId(spark, null)
              }
            }
          },
          "checkpoint-cancelled-candidate-waiter"
        )
        waiterThread.start()
        waiterStarted.await(10, TimeUnit.SECONDS) shouldBe true
        awaitSynchronousRequestCount(spark, requestsBefore + 2)
        awaitDestinationQueue(spark)
        waiterThread.isAlive shouldBe true
        waiterThread.interrupt()
        waiterThread.join(TimeUnit.SECONDS.toMillis(10))
        waiterThread.isAlive shouldBe false
        waiterFailure.get() shouldBe a[InterruptedException]
        waiterInterrupted.get() shouldBe true
        val cancelled = receipts.asScala.find(_.requestId == "cancelled-candidate-waiter").get
        cancelled.outcome shouldBe "waiter_cancelled"
        cancelled.acknowledged shouldBe false
        val cancelledEvidence = evidence.asScala.flatMap(_.requests).find(_.outcome.contains("waiter_cancelled")).get
        cancelledEvidence.status shouldBe "cancelled"
        cancelledEvidence.coalesced shouldBe false
        cancelledEvidence.capturedMutationGeneration shouldBe None
        cancelledEvidence.publishedMutationGeneration shouldBe None
        cancelledEvidence.acknowledgedSnapshotGenerationId shouldBe None

        releaseCapture.countDown()
        Await.result(activeRequest, 60.seconds)
        val activeReceipt = receipts.asScala.find(_.requestId == "active-candidate-capture").get
        val activeCommit  = commitDetails(spark, remote.toURI.toString)
        activeReceipt.acknowledged shouldBe true
        activeReceipt.published shouldBe true
        activeReceipt.reused shouldBe false
        activeReceipt.publicationGeneration shouldBe Some(activeCommit._1)
        activeReceipt.archiveSha256 shouldBe Some(activeCommit._2)
        activeReceipt.archiveBytes shouldBe Some(activeCommit._3)
        commitDetailsFor(spark, remote.toURI.toString, activeReceipt.publicationGeneration.get) shouldBe activeCommit
      } finally {
        releaseCapture.countDown()
        OpenIvmStateSync.setBeforeCaptureHookForTesting(null)
        if (waiterThread != null && waiterThread.isAlive) {
          waiterThread.interrupt()
          waiterThread.join(TimeUnit.SECONDS.toMillis(10))
        }
        if (activeRequest != null)
          try Await.ready(activeRequest, 60.seconds)
          catch { case _: Throwable => () }
        awaitBackupIdle()
        OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
      }
    }

    it("reuses committed coverage with a disjoint sink, and reports capture and commit failures honestly") {
      val fixtureRoot  = newDir("telemetry-coverage")
      val remote       = new File(fixtureRoot, "remote")
      val stateRoot    = new File(fixtureRoot, "state")
      val telemetryDir = new File(fixtureRoot, "telemetry/not-yet-created")
      val spark = newSpark(
        "checkpoint-telemetry-coverage",
        remote,
        Seq(
          FeatureGate.StatePathKey           -> stateRoot.getAbsolutePath,
          FeatureGate.OneLakeTelemetryDirKey -> telemetryDir.toURI.toString
        )
      )
      OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe true
      remote.exists() shouldBe false
      // Use real captured files without waking the RocksDB maintenance thread between generation samples.
      val marker = OpenIvmStatePaths.openIvmRoot(spark).resolve("telemetry-canary-state")
      def mutate(value: String): Long = {
        val precedingGeneration = OpenIvmStateBarrier.currentMutationGeneration(spark)
        OpenIvmStateBarrier.withMutation(spark) {
          Files.createDirectories(marker.getParent)
          Files.write(marker, value.getBytes(StandardCharsets.UTF_8))
        }
        val generation = OpenIvmStateBarrier.currentMutationGeneration(spark)
        generation shouldBe precedingGeneration + 1L
        generation
      }
      val firstMutation = mutate("first")
      val evidence      = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => { evidence.add(value); () })
      def backup(id: String): Unit = {
        setRequestId(spark, id)
        OpenIvmStateSync.backupNow(spark)
      }
      try {
        backup("telemetry-first")
        val firstCommit = commitDetails(spark, remote.toURI.toString)
        remote.isDirectory shouldBe true
        // Exercise an explicit trailing-slash spelling as well as the now-existing directory.
        spark.conf.set(FeatureGate.StateSyncUriKey, remote.toURI.toString)
        backup("telemetry-reuse")
        committedGenerations(spark, remote.toURI.toString) shouldBe Seq(firstCommit._1)

        val secondMutation = mutate("second")
        secondMutation shouldBe firstMutation + 1L
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(() => throw new java.io.IOException("injected"))
        intercept[java.io.IOException](backup("telemetry-failed"))
        committedGenerations(spark, remote.toURI.toString) shouldBe Seq(firstCommit._1)
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
        OpenIvmStateSync.setAfterCandidatePublicationHookForTesting(() => throw new InterruptedException("post-commit"))
        backup("telemetry-retry")
        Thread.interrupted() shouldBe true
        val secondCommit = commitDetails(spark, remote.toURI.toString)
        committedGenerations(spark, remote.toURI.toString) shouldBe Seq(firstCommit._1, secondCommit._1)

        val publications = evidence.asScala.toVector.flatMap(_.publications)
        publications.map(_.status) shouldBe Seq("success", "failed", "success")
        publications.map(_.snapshotGenerationId) shouldBe Seq(Some(firstCommit._1), None, Some(secondCommit._1))
        publications.map(_.requestedMutationGeneration) shouldBe Seq(
          Some(firstMutation),
          Some(secondMutation),
          Some(secondMutation)
        )
        publications.map(_.capturedMutationGeneration) shouldBe publications.map(_.requestedMutationGeneration)
        publications.map(_.publishedMutationGeneration) shouldBe Seq(Some(firstMutation), None, Some(secondMutation))
        publications(1).phases.find(_.operation == "spark.backup.commit").map(_.status) shouldBe Some("failed")

        val requests = evidence.asScala.toVector.flatMap(_.requests)
        requests.map(_.outcome.get) shouldBe Seq(
          "published_covered_generation",
          "reused_committed_coverage",
          "publication_failed",
          "published_covered_generation"
        )
        requests.map(_.acknowledgedSnapshotGenerationId) shouldBe Seq(
          Some(firstCommit._1),
          Some(firstCommit._1),
          None,
          Some(secondCommit._1)
        )
        requests(1).publicationId shouldBe publications.head.publicationId
        requests(1).reused shouldBe true
        requests(1).capturedMutationGeneration shouldBe None
        requests(1).publishedMutationGeneration shouldBe Some(firstMutation)
        requests(2).status shouldBe "failed"
        requests(2).capturedMutationGeneration shouldBe Some(secondMutation)
        requests.map(_.requestId).distinct.size shouldBe 4

        val telemetryFiles = Files.walk(telemetryDir.toPath)
        try telemetryFiles.iterator().asScala.exists(_.getFileName.toString.endsWith(".jsonl")) shouldBe true
        finally telemetryFiles.close()
      } finally {
        Thread.interrupted()
        setRequestId(spark, null)
        OpenIvmStateSync.setBeforeSnapshotCommitHookForTesting(null)
        OpenIvmStateSync.setAfterCandidatePublicationHookForTesting(null)
        OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
      }
    }

    it("binds concurrent traced waiters to the one real coalesced publication covering both generations") {
      implicit val executionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
      val fixture                                                      = newDir("traced-coalescing")
      val remote                                                       = new File(fixture, "remote")
      val state                                                        = new File(fixture, "state")
      val spark       = newSpark("traced-coalescing", remote, Seq(FeatureGate.StatePathKey -> state.getAbsolutePath))
      val evidence    = new CopyOnWriteArrayList[OpenIvmExecutionSpan.StateSyncEvidence]()
      val firstCopy   = new CountDownLatch(1)
      val releaseCopy = new CountDownLatch(1)
      val blocked     = new AtomicBoolean(false)
      OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(value => { evidence.add(value); () })
      OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting(_ => {
        if (blocked.compareAndSet(false, true)) {
          firstCopy.countDown()
          releaseCopy.await(30, TimeUnit.SECONDS) shouldBe true
        }
      })
      val pending = scala.collection.mutable.ArrayBuffer.empty[Future[Unit]]
      def request(parent: String): Unit = {
        pending += Future {
          OpenIvmExecutionSpan.start("canary.profile_only", "refresh", requestId = Some(parent))
          try OpenIvmStateSync.backupNow(spark)
          finally OpenIvmExecutionSpan.finishActive("done", Thread.currentThread().getName)
        }
        ()
      }
      try {
        recordProfile(spark, "first")
        val initial = OpenIvmStateSync.synchronousRequestCountForTesting(spark)
        request("initial")
        firstCopy.await(30, TimeUnit.SECONDS) shouldBe true
        recordProfile(spark, "second")
        val second = OpenIvmStateBarrier.currentMutationGeneration(spark)
        request("second")
        awaitSynchronousRequestCount(spark, initial + 2)
        recordProfile(spark, "third")
        val third = OpenIvmStateBarrier.currentMutationGeneration(spark)
        third should be > second
        request("third")
        awaitSynchronousRequestCount(spark, initial + 3)
        releaseCopy.countDown()
        pending.foreach(future => Await.result(future, 60.seconds))

        val records      = evidence.asScala.toVector
        val publications = records.flatMap(_.publications)
        publications.size shouldBe 2
        val coalesced = publications.find(_.requestedRequests == 2).get
        coalesced.requestedMutationGeneration shouldBe Some(third)
        coalesced.capturedMutationGeneration shouldBe Some(third)
        coalesced.publishedMutationGeneration shouldBe Some(third)
        val requests = records.flatMap(_.requests).filter(_.coalesced)
        requests.map(_.requestId).toSet shouldBe coalesced.requestIds.toSet
        requests.map(_.parentRequestId.get).toSet shouldBe Set("second", "third")
        requests.map(_.requestedMutationGeneration.get).toSet shouldBe Set(second, third)
        requests.foreach { waiter =>
          waiter.reused shouldBe false
          waiter.coverageProven shouldBe true
          waiter.publicationId shouldBe coalesced.publicationId
          waiter.acknowledgedSnapshotGenerationId shouldBe coalesced.snapshotGenerationId
        }
        committedGenerations(spark, remote.toURI.toString).size shouldBe 2
      } finally {
        releaseCopy.countDown()
        pending.foreach(future => Await.ready(future, 60.seconds))
        OpenIvmStateSync.setBeforeLocalFileCopyHookForTesting(null)
        OpenIvmExecutionSpan.setStateSyncEvidenceHookForTesting(null)
      }
    }

    it("captures afresh for telemetry inside state and rejects ambiguous or aliased sinks") {
      val fixtureRoot = newDir("telemetry-overlap")
      val remote      = new File(fixtureRoot, "remote")
      val stateRoot   = new File(fixtureRoot, "state")
      val captured    = new File(stateRoot, "_openivm")
      val alias       = new File(fixtureRoot, "state-alias")
      captured.mkdirs()
      val spark = newSpark(
        "checkpoint-telemetry-overlap",
        remote,
        Seq(
          FeatureGate.StatePathKey           -> stateRoot.getAbsolutePath,
          FeatureGate.OneLakeTelemetryDirKey -> new File(captured, "receipts").toURI.toString
        )
      )
      val root = captured.toPath.toRealPath()
      val conf = spark.sessionState.newHadoopConf()
      OpenIvmStateBarrier.telemetrySinkDisjoint("Files/ambiguous", conf, root) shouldBe false
      OpenIvmStateBarrier.telemetrySinkDisjoint(fixtureRoot.toURI.toString, conf, root) shouldBe false
      Files.createSymbolicLink(alias.toPath, root)
      try {
        OpenIvmStateBarrier.telemetrySinkDisjoint(
          new File(alias, "future/receipts").toURI.toString,
          conf,
          root
        ) shouldBe false
      } finally Files.delete(alias.toPath)
      OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe false

      OpenIvmStateBarrier.withMutation(spark) {
        Files.write(captured.toPath.resolve("tracked-mutation"), Array[Byte](1))
      }
      val generation = OpenIvmStateBarrier.currentMutationGeneration(spark)
      OpenIvmStateSync.backupNow(spark)
      val first = commitDetails(spark, remote.toURI.toString)
      OpenIvmStateSync.backupNow(spark)
      val second = commitDetails(spark, remote.toURI.toString)
      OpenIvmStateBarrier.currentMutationGeneration(spark) shouldBe generation
      first._1 should not be second._1
      receipts.asScala.toVector.map(_.outcome) shouldBe Seq.fill(2)("published_fresh_fallback")
      val archive = new File(remote, s".openivm-state-sync/snapshots/${second._1}/snapshot.zip")
      val zip     = new java.util.zip.ZipFile(archive)
      try zip.entries().asScala.exists(_.getName.endsWith(".jsonl")) shouldBe true
      finally zip.close()
    }

    it("accounts for independent queue writes without a span, profile, or synchronous sink") {
      Seq("disabled", "disjoint", "overlap", "unknown", "missing", "dual-unknown").foreach { arm =>
        val fixture = newDir(s"queue-sink-$arm")
        val remote  = new File(fixture, "remote")
        val state   = new File(fixture, "state")
        val sink    = if (arm == "disjoint") new File(fixture, "sink") else new File(state, "_openivm/queue")
        val destinations = arm match {
          case "unknown" => Seq(BoundedTelemetryQueue.UriKey -> "file:relative/queue")
          case "missing" => Seq.empty
          case "dual-unknown" =>
            Seq(
              BoundedTelemetryQueue.LocalUriKey   -> new File(fixture, "sink").toURI.toString,
              BoundedTelemetryQueue.OneLakeUriKey -> "abfss://workspace@onelake.example/Files/telemetry",
              "spark.hadoop.fs.abfss.impl"        -> classOf[org.apache.hadoop.fs.LocalFileSystem].getName
            )
          case _ => Seq(BoundedTelemetryQueue.UriKey -> sink.toURI.toString)
        }
        val spark = newSpark(
          s"checkpoint-queue-$arm",
          remote,
          Seq(
            FeatureGate.StatePathKey         -> state.getAbsolutePath,
            BoundedTelemetryQueue.EnabledKey -> (arm != "disabled").toString
          ) ++ destinations
        )
        val reusable = arm == "disabled" || arm == "disjoint"
        try {
          // SQL runtime settings cannot turn off a driver worker already configured by SparkConf.
          spark.conf.set(BoundedTelemetryQueue.EnabledKey, "false")
          OpenIvmStateBarrier.mutationCoverageEnabled(spark) shouldBe reusable
          OpenIvmExecutionSpan.stateSyncCaptureEnabled(spark, None) shouldBe (arm != "disabled")
          // An explicit tracked mutation keeps RocksDB's registration-triggered maintenance out of this queue test.
          val marker = OpenIvmStatePaths.openIvmRoot(spark).resolve("queue-canary-state")
          OpenIvmStateBarrier.withMutation(spark) {
            Files.createDirectories(marker.getParent)
            Files.write(marker, arm.getBytes(StandardCharsets.UTF_8))
          }
          val generation = OpenIvmStateBarrier.currentMutationGeneration(spark)
          OpenIvmStateSync.backupNow(spark)
          val first = commitDetails(spark, remote.toURI.toString)
          withClue(s"$arm after initial capture: ") {
            OpenIvmStateBarrier.currentMutationGeneration(spark) shouldBe generation
          }
          if (arm == "overlap" || arm == "disjoint") {
            val sc    = spark.sparkContext
            val queue = BoundedTelemetryQueue.configured(sc.getConf, sc.hadoopConfiguration).get
            try queue.offer("""{"checkpoint_queue_canary":true}""") shouldBe true
            finally queue.shutdown(5000L) shouldBe true
            queue.health.published shouldBe 1L
          }
          OpenIvmStateSync.backupNow(spark)
          val second = commitDetails(spark, remote.toURI.toString)
          withClue(s"$arm after queue activity: ") {
            OpenIvmStateBarrier.currentMutationGeneration(spark) shouldBe generation
            val commits = committedGenerations(spark, remote.toURI.toString)
            if (reusable) commits shouldBe Seq(first._1)
            else commits shouldBe Seq(first._1, second._1)
          }
          if (arm == "overlap") {
            val archive = new File(remote, s".openivm-state-sync/snapshots/${second._1}/snapshot.zip")
            val zip     = new java.util.zip.ZipFile(archive)
            try {
              Option(zip.getEntry("queue-canary-state")) should not be empty
              zip
                .entries()
                .asScala
                .exists(entry => entry.getName.startsWith("queue/") && entry.getName.endsWith(".jsonl")) shouldBe true
            } finally zip.close()
          }
        } finally stopSpark(spark)
      }
    }

    it("exports actual publish and reuse evidence in queue-only mode without changing the checkpoint path") {
      val fixture = newDir("queue-only-evidence")
      val remote  = new File(fixture, "remote")
      val state   = new File(fixture, "state")
      val sink    = new File(fixture, "sink")
      val spark = newSpark(
        "checkpoint-queue-only-evidence",
        remote,
        Seq(
          FeatureGate.StatePathKey         -> state.getAbsolutePath,
          BoundedTelemetryQueue.EnabledKey -> "true",
          BoundedTelemetryQueue.UriKey     -> sink.toURI.toString
        )
      )
      val sc    = spark.sparkContext
      val queue = BoundedTelemetryRuntime.start(sc.getConf, sc.hadoopConfiguration).get
      try {
        OpenIvmExecutionSpan.captureCurrent() shouldBe None
        FeatureGate.telemetryUri(spark) shouldBe None
        FeatureGate.oneLakeTelemetryDir(spark) shouldBe None
        FeatureGate.profileRefreshEnabled(spark) shouldBe false
        recordProfile(spark, "queue-only-evidence")
        sc.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, "refresh-evidence")
        OpenIvmStateSync.backupNow(spark)
        OpenIvmStateSync.backupNow(spark)
      } finally {
        sc.setLocalProperty(OpenIvmExecutionSpan.TraceOperationIdProperty, null)
        BoundedTelemetryRuntime.stop(queue, 5000L) shouldBe true
      }

      val generations = committedGenerations(spark, remote.toURI.toString)
      generations should have size 1
      val generation = generations.head
      val json       = new com.fasterxml.jackson.databind.ObjectMapper()
      val files      = Files.list(sink.toPath)
      val evidence =
        try
          files
            .iterator()
            .asScala
            .filter(_.getFileName.toString.endsWith(".jsonl"))
            .map(path => json.readTree(path.toFile).path("state_sync"))
            .toVector
        finally files.close()
      val requests = evidence.flatMap(_.path("requests").elements().asScala)
      requests should have size 2
      requests.map(_.path("parent_operation_id").asText()).distinct shouldBe Seq("refresh-evidence")
      requests.map(_.path("outcome").asText()).toSet shouldBe Set(
        "published_covered_generation",
        "reused_committed_coverage"
      )
      requests.map(_.path("acknowledged_snapshot_generation_id").asText()).distinct shouldBe Seq(generation)
      val publications = evidence.flatMap(_.path("publications").elements().asScala)
      publications.map(_.path("snapshot_generation_id").asText()) shouldBe Seq(generation)
      publications.head.path("published_mutation_generation").isIntegralNumber shouldBe true
    }

    it("invalidates cached coverage when a runtime queue writes and exits between checkpoint requests") {
      val fixture = newDir("queue-topology")
      val remote  = new File(fixture, "remote")
      val state   = new File(fixture, "state")
      val spark = newSpark("checkpoint-queue-topology", remote, Seq(FeatureGate.StatePathKey -> state.getAbsolutePath))
      recordProfile(spark, "queue-topology")
      OpenIvmStateSync.backupNow(spark)
      val first = commitDetails(spark, remote.toURI.toString)
      val sc    = spark.sparkContext
      val config = sc.getConf
        .set(BoundedTelemetryQueue.EnabledKey, "true")
        .set(BoundedTelemetryQueue.UriKey, new File(state, "_openivm/queue").toURI.toString)
      val queue = BoundedTelemetryRuntime.start(config, sc.hadoopConfiguration).get
      try queue.offer("""{"checkpoint_queue_epoch_canary":true}""") shouldBe true
      finally BoundedTelemetryRuntime.stop(queue, 5000L) shouldBe true
      queue.health.published shouldBe 1L

      OpenIvmStateSync.backupNow(spark)
      val second = commitDetails(spark, remote.toURI.toString)
      second._1 should not be first._1
      val archive = new File(remote, s".openivm-state-sync/snapshots/${second._1}/snapshot.zip")
      val zip     = new java.util.zip.ZipFile(archive)
      try
        zip
          .entries()
          .asScala
          .exists(entry => entry.getName.startsWith("queue/") && entry.getName.endsWith(".jsonl")) shouldBe true
      finally zip.close()
    }

    it("isolates cached coverage when distinct state roots share one publication destination") {
      val fixtureRoot = newDir("shared-destination")
      val remote      = new File(fixtureRoot, "remote")
      remote.mkdirs()
      val stateA = new File(fixtureRoot, "state-a")
      val stateB = new File(fixtureRoot, "state-b")
      stateA.mkdirs()
      stateB.mkdirs()

      val a1 = newSpark(
        "checkpoint-root-a-first",
        remote,
        Seq(FeatureGate.StatePathKey -> stateA.getAbsolutePath)
      )
      recordProfile(a1, "root-a-before")
      setRequestId(a1, "root-a-before")
      OpenIvmStateSync.backupNow(a1)
      val first = receipts.asScala.find(_.requestId == "root-a-before").get
      first.published shouldBe true
      first.coverageProven shouldBe true
      setRequestId(a1, "root-a-no-change")
      OpenIvmStateSync.backupNow(a1)
      val noChange = receipts.asScala.find(_.requestId == "root-a-no-change").get
      noChange.published shouldBe false
      noChange.reused shouldBe true
      noChange.coverageProven shouldBe true
      noChange.publicationGeneration shouldBe first.publicationGeneration
      noChange.archiveSha256 shouldBe first.archiveSha256
      noChange.archiveBytes shouldBe first.archiveBytes
      stopSpark(a1)

      val sentinelDbPath = new File(stateB, "_openivm/sentinel/rocksdb").getAbsolutePath
      val sentinelDb     = new OpenIvmRocksDB(sentinelDbPath, OpenIvmRocksDBConf.default, Seq("meta"))
      sentinelDb.load()
      sentinelDb.withBatch { batch =>
        sentinelDb.put(batch, "meta", RocksDBCodec.utf8("state-b"), RocksDBCodec.utf8("local"))
      }
      sentinelDb.close()

      val b = newSpark(
        "checkpoint-root-b",
        remote,
        Seq(
          FeatureGate.StatePathKey    -> stateB.getAbsolutePath,
          FeatureGate.StateSyncUriKey -> remote.getAbsolutePath
        )
      )
      RefreshProfileCatalog.ensureTables(b)
      RefreshProfileCatalog.record(
        b,
        Seq(
          RefreshProfileRow(
            "root-b-only",
            "canary.profile_only",
            Timestamp.valueOf("2025-01-01 00:00:00"),
            0,
            "profile_only_fixture",
            7L,
            "operation_id=root-b-only"
          )
        )
      )
      RefreshProfileCatalog.scanAll(b).map(_.refreshId) shouldBe Seq("root-b-only")
      setRequestId(b, "root-b-only")
      OpenIvmStateSync.backupNow(b)
      val second = receipts.asScala.find(_.requestId == "root-b-only").get
      second.published shouldBe true
      second.reused shouldBe false
      stopSpark(b)

      val a2 = newSpark(
        "checkpoint-root-a-after-sharing",
        remote,
        Seq(FeatureGate.StatePathKey -> stateA.getAbsolutePath)
      )
      recordProfile(a2, "root-a-after-sharing")
      setRequestId(a2, "root-a-after-sharing")
      OpenIvmStateSync.backupNow(a2)
      val third = receipts.asScala.find(_.requestId == "root-a-after-sharing").get
      third.published shouldBe true
      third.reused shouldBe false
      third.acknowledged shouldBe true
      third.publicationGeneration should not be second.publicationGeneration
      val latest = commitDetails(a2, remote.toURI.toString)
      latest._1 shouldBe third.publicationGeneration.get
      third.archiveSha256 shouldBe Some(latest._2)
      third.archiveBytes shouldBe Some(latest._3)
      val publishedIds = receipts.asScala.filter(receipt =>
        receipt.requestId == "root-a-before" || receipt.requestId == "root-a-no-change" ||
          receipt.requestId == "root-b-only" || receipt.requestId == "root-a-after-sharing"
      )
      publishedIds.count(_.published) shouldBe 3
      publishedIds.count(_.reused) shouldBe 1
      publishedIds.find(_.requestId == "root-a-after-sharing").get.reused shouldBe false

      writeReceiptEvidence(
        "shared-state-roots",
        Seq("root-a-before", "root-a-no-change", "root-b-only", "root-a-after-sharing"),
        latest,
        "\"distinct_roots\":2,\"shared_destination_fallback\":true," +
          "\"cross_root_reuse\":false,\"same_root_no_change_reuse\":true"
      )
    }
  }
}
