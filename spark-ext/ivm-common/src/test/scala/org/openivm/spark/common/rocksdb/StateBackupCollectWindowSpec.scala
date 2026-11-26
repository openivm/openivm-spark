package org.openivm.spark.common.rocksdb

import org.openivm.spark.common.{DeltaGroupCommit, FeatureGate}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

/** A legacy synchronous state backup led by a refresh may wait only for same-state refreshes that already reached a
  * group commit, and every caller is still acknowledged or rejected by the pass that served it.
  */
class StateBackupCollectWindowSpec extends AnyFunSpec with BeforeAndAfterEach with BeforeAndAfterAll with Matchers {
  private val TestStateSyncKey = "spark.openivm.test.stateSync.key"
  private val Uri              = "abfss://state-sync@onelake/lh/_openivm"
  private val Timeout          = 30.seconds

  private val sparks    = ArrayBuffer.empty[SparkSession]
  private val dirs      = ArrayBuffer.empty[File]
  private val refreshes = ArrayBuffer.empty[Refresh]

  private implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  override def afterEach(): Unit =
    try {
      refreshes.foreach(_.proceed())
      refreshes.foreach(refresh => Await.ready(refresh.result, Timeout))
    } finally {
      refreshes.clear()
      sparks.reverse.foreach(spark => scala.util.Try(spark.stop()))
      sparks.clear()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      OpenIvmStateSync.resetForTesting()
      dirs.reverse.foreach(deleteRecursively)
      dirs.clear()
      super.afterEach()
    }

  override def afterAll(): Unit =
    try ec.shutdownNow()
    finally super.afterAll()

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()
  }

  private def newSpark(appName: String, extraConf: Seq[(String, String)] = Seq.empty): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val warehouse = new File(s"target/test-backup-window-$appName-${UUID.randomUUID().toString.take(8)}")
    warehouse.mkdirs()
    dirs += warehouse
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      .config(FeatureGate.StateSyncUriKey, Uri)
      .config(FeatureGate.StateSyncCheckpointEnabledKey, "false")
    extraConf.foreach { case (key, value) => builder.config(key, value) }
    val spark = builder.getOrCreate()
    sparks += spark
    spark
  }

  private def stateSession(base: SparkSession, key: String, async: Boolean = false): SparkSession = {
    val session = base.newSession()
    session.conf.set(TestStateSyncKey, key)
    session.conf.set(FeatureGate.StateSyncAsyncEnabledKey, async.toString)
    session
  }

  private def installHooks(pass: () => Unit): Unit = {
    OpenIvmStateSync.setStateSyncKeyHookForTesting((session, _) => session.conf.get(TestStateSyncKey))
    OpenIvmStateSync.setBackupPassHookForTesting((_, _, _) => pass())
  }

  private def commitPath(): String = s"backup-window-${UUID.randomUUID()}"

  private def collectWaits: Long = OpenIvmMetrics.timer("state_sync.collect_wait").getCount

  private def requests(session: SparkSession): Long = OpenIvmStateSync.synchronousRequestCountForTesting(session)

  private def committingUnder(key: String): Int =
    DeltaGroupCommit.unseenCommittingRefreshes(key, new java.util.HashSet[AnyRef]())

  private def eventually(condition: => Boolean): Unit = {
    val deadline = Timeout.fromNow
    while (!condition && deadline.hasTimeLeft()) LockSupport.parkNanos(1000000L)
    condition shouldBe true
  }

  private def failureOf(refresh: Refresh): Throwable =
    Await.ready(refresh.result, Timeout).value.get.failed.get

  /** Scala 2.12 futures never complete on a fatal `InterruptedException`, so a refresh reports it wrapped. */
  private final class RefreshInterrupted(cause: InterruptedException) extends Exception(cause)

  /** A refresh command on its own thread: becomes ready, declares its synchronous backup, reaches the group commit at
    * `path` unless `commit` is false, then waits for [[proceed]] and backs up unless `backup` is false. Its result is
    * whether the thread was left interrupted.
    */
  private final class Refresh(session: SparkSession, path: String, commit: Boolean, backup: Boolean) {
    private val committed = new CountDownLatch(1)
    private val release   = new CountDownLatch(1)
    private val thread    = new AtomicReference[Thread]()

    val result: Future[Boolean] = Future {
      thread.set(Thread.currentThread())
      val arrival = DeltaGroupCommit.refreshStarted()
      try {
        arrival.ready()
        OpenIvmStateSync.expectSynchronousBackup(session)
        if (commit) DeltaGroupCommit.forPath[Int](path).submit(1)(_ => ())(_ => ())
        committed.countDown()
        release.await(Timeout.toMillis, TimeUnit.MILLISECONDS)
        if (backup) OpenIvmStateSync.backupNow(session)
        Thread.interrupted()
      } catch { case interrupted: InterruptedException => throw new RefreshInterrupted(interrupted) }
      finally arrival.close()
    }

    def awaitCommitted(): Refresh = {
      committed.await(Timeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
      this
    }

    def proceed(): Unit = release.countDown()

    def interrupt(): Unit = thread.get().interrupt()

    def awaitResult(): Boolean = Await.result(result, Timeout)
  }

  /** Starts a refresh and waits until it has committed, so refreshes reach the shared group commit one at a time. */
  private def refresh(session: SparkSession, path: String, commit: Boolean = true, backup: Boolean = true): Refresh = {
    val started = new Refresh(session, path, commit, backup)
    refreshes += started
    started.awaitCommitted()
  }

  describe("legacy synchronous state backup led by a refresh") {
    it("does not wait for refreshes of another state or for a refresh that never commits") {
      val spark  = newSpark("unrelated")
      val stateA = stateSession(spark, "state-a")
      val stateB = stateSession(spark, "state-b")
      val passes = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path   = commitPath()
      val leader = refresh(stateA, path)
      refresh(stateB, path, backup = false)
      refresh(stateA, path, commit = false, backup = false)
      committingUnder("state-b") shouldBe 1
      committingUnder("state-a") shouldBe 1
      val waits = collectWaits

      leader.proceed()
      leader.awaitResult() shouldBe false
      passes.get() shouldBe 1
      collectWaits shouldBe waits
    }

    it("acknowledges the leader with a single pass when the refreshes it waited for end without a backup") {
      val session = stateSession(newSpark("abandoned"), "abandoned")
      val passes  = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path    = commitPath()
      val leader  = refresh(session, path)
      val awaited = (1 to 2).map(_ => refresh(session, path, backup = false))
      val waits   = collectWaits

      leader.proceed()
      eventually(requests(session) == 1L)
      awaited.foreach(_.proceed())

      leader.awaitResult() shouldBe false
      awaited.foreach(_.awaitResult() shouldBe false)
      passes.get() shouldBe 1
      collectWaits shouldBe waits + 1
    }

    it("never waits when a caller outside a refresh leads the pass") {
      val session = stateSession(newSpark("non-refresh"), "non-refresh")
      val passes  = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      refresh(session, commitPath(), backup = false)
      committingUnder("non-refresh") shouldBe 1
      val waits = collectWaits

      (1 to 3).foreach(_ => OpenIvmStateSync.backupNow(session))
      passes.get() shouldBe 3
      collectWaits shouldBe waits
    }

    it("still publishes and acknowledges a leader interrupted while waiting, and restores its interrupt") {
      val session     = stateSession(newSpark("interrupt"), "interrupt")
      val passes      = new AtomicInteger(0)
      val interrupted = new AtomicBoolean(false)
      installHooks { () =>
        if (passes.incrementAndGet() == 1) {
          val deadline = Timeout.fromNow
          while (!interrupted.get() && deadline.hasTimeLeft()) LockSupport.parkNanos(1000000L)
        }
      }
      val path    = commitPath()
      val leader  = refresh(session, path)
      val awaited = refresh(session, path)

      leader.proceed()
      eventually(requests(session) == 1L)
      leader.interrupt()
      interrupted.set(true)

      leader.awaitResult() shouldBe true
      passes.get() shouldBe 1

      awaited.proceed()
      awaited.awaitResult() shouldBe false
      passes.get() shouldBe 2
    }

    it("rejects every refresh served by a failed pass and acknowledges a later retry") {
      val session = stateSession(newSpark("failure"), "failure")
      val failing = new AtomicBoolean(true)
      val failure = new java.io.IOException("snapshot upload failed")
      installHooks(() => if (failing.get()) throw failure)
      val path   = commitPath()
      val leader = refresh(session, path)
      val joiner = refresh(session, path)

      leader.proceed()
      eventually(requests(session) == 1L)
      joiner.proceed()

      failureOf(leader) shouldBe failure
      failureOf(joiner) shouldBe failure

      failing.set(false)
      OpenIvmStateSync.backupNow(session)
    }

    it("does not wait for an asynchronous refresh of the same state") {
      val spark  = newSpark("mixed")
      val legacy = stateSession(spark, "mixed")
      val async  = stateSession(spark, "mixed", async = true)
      val passes = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path   = commitPath()
      val leader = refresh(legacy, path)
      refresh(async, path, backup = false)
      committingUnder("mixed") shouldBe 1
      val waits = collectWaits

      leader.proceed()
      leader.awaitResult() shouldBe false
      passes.get() shouldBe 1
      collectWaits shouldBe waits
    }

    it("does not register checkpoint-mode refreshes for the window") {
      val session = stateSession(
        newSpark("checkpoint", Seq(FeatureGate.StateSyncCheckpointEnabledKey -> "true")),
        "checkpoint"
      )
      installHooks(() => ())
      refresh(session, commitPath(), backup = false)
      committingUnder("checkpoint") shouldBe 0
    }

    it("does not register guarded refreshes for the window") {
      val session = stateSession(
        newSpark("guarded", Seq(FeatureGate.GuardedStateSyncUriKey -> "abfss://guarded@onelake/lh/_openivm")),
        "guarded"
      )
      installHooks(() => ())
      refresh(session, commitPath(), backup = false)
      committingUnder("guarded") shouldBe 0
    }
  }
}
