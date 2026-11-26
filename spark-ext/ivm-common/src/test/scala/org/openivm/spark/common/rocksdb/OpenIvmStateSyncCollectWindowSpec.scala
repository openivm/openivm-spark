package org.openivm.spark.common.rocksdb

import org.openivm.spark.common.{DeltaGroupCommit, FeatureGate}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

/** Legacy synchronous state-backup collect window: a refresh that leads a pass waits, within the pass-cost bound, only
  * for same-state refreshes that already reached a group commit, and its bookkeeping stays bounded by the refreshes in
  * flight whoever leads.
  */
class OpenIvmStateSyncCollectWindowSpec
    extends AnyFunSpec
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers {
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
    val warehouse = new File(s"target/test-collect-window-$appName-${UUID.randomUUID().toString.take(8)}")
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

  private def commitPath(): String = s"collect-window-${UUID.randomUUID()}"

  private def collectWaits: Long = OpenIvmMetrics.timer("state_sync.collect_wait").getCount

  private def requests(session: SparkSession): Long = OpenIvmStateSync.synchronousRequestCountForTesting(session)

  private def committingUnder(key: String): Int =
    DeltaGroupCommit.unseenCommittingRefreshes(key, new java.util.HashSet[AnyRef]())

  private def arrivedRefreshCount(key: String): Int = {
    def field(owner: AnyRef, suffix: String): AnyRef = {
      val declared = owner.getClass.getDeclaredFields.find(_.getName.endsWith(suffix)).get
      declared.setAccessible(true)
      declared.get(owner)
    }
    val state = field(OpenIvmStateSync, "backupStates").asInstanceOf[java.util.Map[String, AnyRef]].get(key)
    field(state, "arrivedRefreshes").asInstanceOf[java.util.Set[_]].size()
  }

  private def waitUntil(timeout: FiniteDuration = 10.seconds)(condition: => Boolean): Unit = {
    val deadline = timeout.fromNow
    while (!condition && deadline.hasTimeLeft()) Thread.sleep(5L)
    condition shouldBe true
  }

  private def failureOf(refresh: Refresh): Throwable =
    Await.ready(refresh.result, Timeout).value.get.failed.get

  /** Scala 2.12 futures never complete on a fatal `InterruptedException`, so a refresh reports it wrapped. */
  private final class RefreshInterrupted(cause: InterruptedException) extends Exception(cause)

  /** A refresh command on its own thread: becomes ready, declares its synchronous backup, reaches the group commit at
    * `path` unless `commit` is false, then waits for [[proceed]] and backs up unless `backup` is false. Its result is
    * the backup's duration and whether the thread was left interrupted.
    */
  private final class Refresh(session: SparkSession, path: String, commit: Boolean, backup: Boolean) {
    private val committed = new CountDownLatch(1)
    private val release   = new CountDownLatch(1)
    private val thread    = new AtomicReference[Thread]()

    val result: Future[(Long, Boolean)] = Future {
      thread.set(Thread.currentThread())
      val arrival = DeltaGroupCommit.refreshStarted()
      try {
        arrival.ready()
        OpenIvmStateSync.expectSynchronousBackup(session)
        if (commit) DeltaGroupCommit.forPath[Int](path).submit(1)(_ => ())(_ => ())
        committed.countDown()
        release.await(Timeout.toMillis, TimeUnit.MILLISECONDS)
        val started = System.nanoTime()
        if (backup) OpenIvmStateSync.backupNow(session)
        (System.nanoTime() - started, Thread.interrupted())
      } catch { case interrupted: InterruptedException => throw new RefreshInterrupted(interrupted) }
      finally arrival.close()
    }

    def awaitCommitted(): Refresh = {
      committed.await(Timeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
      this
    }

    def proceed(): Unit = release.countDown()

    def interrupt(): Unit = thread.get().interrupt()

    def awaitResult(): (Long, Boolean) = Await.result(result, Timeout)
  }

  /** Starts a refresh and waits until it has committed, so refreshes reach the shared group commit one at a time. */
  private def refresh(session: SparkSession, path: String, commit: Boolean = true, backup: Boolean = true): Refresh = {
    val started = new Refresh(session, path, commit, backup)
    refreshes += started
    started.awaitCommitted()
  }

  describe("legacy synchronous state-backup collect window") {
    it("coalesces a same-state refresh that reaches its backup inside the leader's window") {
      val session = stateSession(newSpark("coalesce"), "coalesce")
      val passes  = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path   = commitPath()
      val leader = refresh(session, path)
      val joiner = refresh(session, path)
      val waits  = collectWaits

      leader.proceed()
      waitUntil()(requests(session) == 1L)
      Thread.sleep(100L)
      passes.get() shouldBe 0
      joiner.proceed()

      val (leaderNanos, _) = leader.awaitResult()
      joiner.awaitResult()
      passes.get() shouldBe 1
      collectWaits shouldBe waits + 1
      leaderNanos should be < 1.second.toNanos
    }

    it("never waits for committing refreshes of another state root or for a refresh that never commits") {
      val spark  = newSpark("unrelated")
      val rootA  = stateSession(spark, "root-a")
      val rootB  = stateSession(spark, "root-b")
      val passes = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path   = commitPath()
      val leader = refresh(rootA, path)
      refresh(rootB, path, backup = false)
      refresh(rootA, path, commit = false, backup = false)
      committingUnder("root-b") shouldBe 1
      committingUnder("root-a") shouldBe 1
      val waits = collectWaits

      leader.proceed()
      val (nanos, _) = leader.awaitResult()
      passes.get() shouldBe 1
      collectWaits shouldBe waits
      nanos should be < 400.millis.toNanos
    }

    it("closes the window as soon as awaited refreshes end without backing up") {
      val session = stateSession(newSpark("closed"), "closed")
      val passes  = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path    = commitPath()
      val leader  = refresh(session, path)
      val failing = (1 to 3).map(_ => refresh(session, path, backup = false))
      val waits   = collectWaits

      leader.proceed()
      waitUntil()(requests(session) == 1L)
      Thread.sleep(150L)
      passes.get() shouldBe 0
      failing.foreach(_.proceed())

      val (nanos, _) = leader.awaitResult()
      passes.get() shouldBe 1
      collectWaits shouldBe waits + 1
      nanos should be < 650.millis.toNanos
    }

    it("bounds the window by the pass cost and serves a late refresh with the next pass") {
      val session      = stateSession(newSpark("late"), "late")
      val passes       = new AtomicInteger(0)
      val firstEntered = new CountDownLatch(1)
      val releaseFirst = new CountDownLatch(1)
      installHooks { () =>
        if (passes.incrementAndGet() == 1) {
          firstEntered.countDown()
          releaseFirst.await(Timeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
        }
      }
      val path   = commitPath()
      val leader = refresh(session, path)
      val late   = refresh(session, path)
      val waits  = collectWaits

      try {
        leader.proceed()
        firstEntered.await(Timeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
        late.proceed()
        waitUntil()(requests(session) == 2L)
        late.result.isCompleted shouldBe false
      } finally releaseFirst.countDown()

      val (leaderNanos, _) = leader.awaitResult()
      late.awaitResult()
      passes.get() shouldBe 2
      collectWaits shouldBe waits + 1
      leaderNanos should be >= 400.millis.toNanos
      leaderNanos should be < 2.seconds.toNanos
    }

    it("publishes and restores the interrupt when the leader is interrupted inside its window") {
      val session = stateSession(newSpark("interrupt"), "interrupt")
      val passes  = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path    = commitPath()
      val leader  = refresh(session, path)
      val awaited = refresh(session, path)
      val waits   = collectWaits

      leader.proceed()
      waitUntil()(requests(session) == 1L)
      Thread.sleep(50L)
      passes.get() shouldBe 0
      leader.interrupt()

      val (nanos, interrupted) = leader.awaitResult()
      interrupted shouldBe true
      passes.get() shouldBe 1
      collectWaits shouldBe waits + 1
      nanos should be < 450.millis.toNanos

      awaited.proceed()
      awaited.awaitResult()
      passes.get() shouldBe 2
    }

    it("fails every refresh of a coalesced pass and acknowledges none of them") {
      val session = stateSession(newSpark("failure"), "failure")
      val passes  = new AtomicInteger(0)
      val failure = new java.io.IOException("snapshot upload failed")
      installHooks(() => if (passes.incrementAndGet() == 1) throw failure)
      val path   = commitPath()
      val leader = refresh(session, path)
      val joiner = refresh(session, path)

      leader.proceed()
      waitUntil()(requests(session) == 1L)
      Thread.sleep(50L)
      joiner.proceed()

      failureOf(leader) shouldBe failure
      failureOf(joiner) shouldBe failure
      passes.get() shouldBe 1

      OpenIvmStateSync.backupNow(session)
      passes.get() shouldBe 2
    }

    it("keeps waits and arrival bookkeeping bounded while non-refresh callers lead every pass") {
      val session = stateSession(newSpark("non-refresh-leader"), "non-refresh-leader")
      val gate    = new AtomicReference[(CountDownLatch, CountDownLatch)]()
      installHooks { () =>
        Option(gate.getAndSet(null)).foreach { case (entered, release) =>
          entered.countDown()
          release.await(Timeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
        }
      }
      val path = commitPath()
      refresh(session, path, backup = false)
      val waits = collectWaits

      (1 to 50).foreach { _ =>
        val entered = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        gate.set((entered, release))
        val leader = Future(OpenIvmStateSync.backupNow(session))
        try {
          entered.await(Timeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
          val follower = refresh(session, path)
          val before   = requests(session)
          follower.proceed()
          waitUntil()(requests(session) == before + 1L)
          follower.interrupt()
          failureOf(follower).getCause shouldBe an[InterruptedException]
        } finally release.countDown()
        Await.result(leader, Timeout)
        arrivedRefreshCount("non-refresh-leader") should be <= 1
      }
      collectWaits shouldBe waits

      val steady = refresh(session, path)
      steady.proceed()
      val (nanos, _) = steady.awaitResult()
      collectWaits shouldBe waits + 1
      nanos should be < 300.millis.toNanos
      arrivedRefreshCount("non-refresh-leader") should be <= 1
    }

    it("tags only legacy synchronous refreshes, so a legacy leader never waits for an asynchronous one") {
      val spark  = newSpark("mixed")
      val legacy = stateSession(spark, "mixed")
      val async  = stateSession(spark, "mixed", async = true)
      val passes = new AtomicInteger(0)
      installHooks(() => { passes.incrementAndGet(); () })
      val path   = commitPath()
      val leader = refresh(legacy, path)
      refresh(async, path, backup = false)
      OpenIvmStateSync.expectSynchronousBackup(legacy)
      committingUnder("mixed") shouldBe 1
      val waits = collectWaits

      leader.proceed()
      val (nanos, _) = leader.awaitResult()
      passes.get() shouldBe 1
      collectWaits shouldBe waits
      nanos should be < 400.millis.toNanos
    }

    it("does not tag checkpoint-mode refreshes") {
      val session = stateSession(
        newSpark("checkpoint", Seq(FeatureGate.StateSyncCheckpointEnabledKey -> "true")),
        "checkpoint"
      )
      installHooks(() => ())
      refresh(session, commitPath(), backup = false)
      committingUnder("checkpoint") shouldBe 0
    }

    it("does not tag guarded refreshes") {
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
