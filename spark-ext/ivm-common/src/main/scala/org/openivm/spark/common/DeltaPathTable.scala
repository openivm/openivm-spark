package org.openivm.spark.common

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import java.util.concurrent.ConcurrentHashMap
import scala.util.control.NonFatal

private[common] object DeltaPathTable {
  def createIfAbsent(spark: SparkSession, path: String, schema: StructType, partitionColumn: String): Unit =
    spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write
      .format("delta")
      .mode("ignore")
      .partitionBy(partitionColumn)
      .save(path)
}

/** Leader/follower group commit for writers of one Delta catalog table in this JVM.
  *
  * A writer enqueues its request. If no commit is in flight it becomes the leader, drains every pending request and
  * commits them together; writers that arrive meanwhile queue behind the in-flight commit and form the next batch.
  * Before draining, a leader may wait briefly for refreshes that hold their locks but have not reached this queue yet
  * (see [[DeltaGroupCommit.RefreshArrival]]). It waits only while the expected saving outweighs the delay: with `k`
  * requests pending, `m` refreshes awaited and a typical commit taking `C`, a straggler that misses the batch waits
  * up to `2C` instead of `C`, so the leader waits at most `C * m / (k + m)`.
  *
  * Every caller returns only after the commit covering its request is durable. A batch of one runs the caller's own
  * single-request path. If a multi-request batch fails, each caller re-runs its own single-request path, so batching
  * never changes what is recorded, only how many Delta commits record it.
  */
private[common] final class DeltaGroupCommit[R] {
  private final class Slot(val request: R) {
    var done: Boolean        = false
    var retrySingle: Boolean = false
    var failure: Throwable   = _
  }

  private val pending      = new java.util.ArrayDeque[Slot]()
  private val arrived      = new java.util.HashSet[AnyRef]()
  private var leaderActive = false
  private var commitNanos  = DeltaGroupCommit.InitialCommitNanos

  def submit(request: R)(single: R => Unit)(batch: Seq[R] => Unit): Unit = {
    val mine        = new Slot(request)
    val refresh     = DeltaGroupCommit.currentRefresh()
    var interrupted = false
    val led = synchronized {
      refresh.foreach(arrived.add)
      pending.addLast(mine)
      notifyAll()
      var drained = Vector.empty[Slot]
      while (drained.isEmpty && !mine.done) {
        if (!leaderActive) {
          leaderActive = true
          if (!collect()) interrupted = true
          while (!pending.isEmpty) drained :+= pending.pollFirst()
        } else
          try wait()
          catch {
            case e: InterruptedException =>
              if (pending.remove(mine)) throw e
              interrupted = true
          }
      }
      drained
    }
    if (led.nonEmpty) lead(mine, led, single, batch)
    if (interrupted) Thread.currentThread().interrupt()
    if (mine.retrySingle) single(request)
    else if (mine.failure != null) throw mine.failure
  }

  /** Holding leadership and this monitor, waits for awaited refreshes within the cost bound. False if interrupted. */
  private def collect(): Boolean = {
    val start = System.nanoTime()
    var open  = true
    while (open) {
      val awaited = DeltaGroupCommit.readyRefreshes - arrived.size
      val budget =
        if (awaited <= 0) 0L
        else math.min(commitNanos, DeltaGroupCommit.MaxWindowNanos) * awaited / (pending.size + awaited)
      val remaining = start + budget - System.nanoTime()
      if (remaining <= 0) open = false
      else
        try wait(math.max(1L, remaining / 1000000L))
        catch { case _: InterruptedException => return false }
    }
    true
  }

  private def forget(refresh: AnyRef): Unit = synchronized {
    arrived.remove(refresh)
    notifyAll()
  }

  private def lead(mine: Slot, slots: Vector[Slot], single: R => Unit, batch: Seq[R] => Unit): Unit = {
    val start     = System.nanoTime()
    var committed = false
    try {
      if (slots.size == 1)
        try { single(mine.request); committed = true }
        catch { case e: Throwable => mine.failure = e }
      else
        try { batch(slots.map(_.request)); committed = true }
        catch {
          case NonFatal(_) => slots.foreach(_.retrySingle = true)
          case e: Throwable =>
            slots.foreach(_.retrySingle = true)
            mine.retrySingle = false
            mine.failure = e
        }
    } finally
      synchronized {
        if (committed) commitNanos = (3 * commitNanos + (System.nanoTime() - start)) / 4
        slots.foreach(_.done = true)
        leaderActive = false
        notifyAll()
      }
  }
}

private[spark] object DeltaGroupCommit {
  private val InitialCommitNanos = 1000000000L
  private val MaxWindowNanos     = 5000000000L

  private val queues        = new ConcurrentHashMap[String, DeltaGroupCommit[_]]()
  private val readyArrivals = ConcurrentHashMap.newKeySet[AnyRef]()
  private val current       = new ThreadLocal[RefreshArrival]()

  /** One refresh command on this thread that may reach a catalog group commit; a nested refresh shares the outer one.
    * Call [[RefreshArrival.ready]] once the refresh holds its locks and [[RefreshArrival.close]] when it ends.
    */
  final class RefreshArrival private[DeltaGroupCommit] (owner: Boolean) {
    def ready(): Unit = if (owner) { readyArrivals.add(this); () }

    def close(): Unit = if (owner) {
      current.remove()
      if (readyArrivals.remove(this)) {
        val it = queues.values.iterator
        while (it.hasNext) it.next().forget(this)
      }
    }
  }

  def refreshStarted(): RefreshArrival =
    if (current.get != null) new RefreshArrival(owner = false)
    else {
      val arrival = new RefreshArrival(owner = true)
      current.set(arrival)
      arrival
    }

  /** The refresh on this thread reaches no further catalog group commit, so open batches stop waiting for it. */
  def departCurrent(): Unit = Option(current.get).foreach { arrival =>
    if (readyArrivals.remove(arrival)) {
      val it = queues.values.iterator
      while (it.hasNext) it.next().forget(arrival)
    }
  }

  private def currentRefresh(): Option[AnyRef] = Option(current.get).filter(readyArrivals.contains)

  private def readyRefreshes: Int = readyArrivals.size

  private[common] def forPath[R](path: String): DeltaGroupCommit[R] =
    queues.computeIfAbsent(path, _ => new DeltaGroupCommit[R]).asInstanceOf[DeltaGroupCommit[R]]
}
