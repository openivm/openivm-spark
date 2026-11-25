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
  private var leaderActive = false

  def submit(request: R)(single: R => Unit)(batch: Seq[R] => Unit): Unit = {
    val mine        = new Slot(request)
    var interrupted = false
    val led = synchronized {
      pending.addLast(mine)
      var drained = Vector.empty[Slot]
      while (drained.isEmpty && !mine.done) {
        if (!leaderActive) {
          leaderActive = true
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

  private def lead(mine: Slot, slots: Vector[Slot], single: R => Unit, batch: Seq[R] => Unit): Unit =
    try {
      if (slots.size == 1)
        try single(mine.request)
        catch { case e: Throwable => mine.failure = e }
      else
        try batch(slots.map(_.request))
        catch {
          case NonFatal(_) => slots.foreach(_.retrySingle = true)
          case e: Throwable =>
            slots.foreach(_.retrySingle = true)
            mine.retrySingle = false
            mine.failure = e
        }
    } finally
      synchronized {
        slots.foreach(_.done = true)
        leaderActive = false
        notifyAll()
      }
}

private[common] object DeltaGroupCommit {
  private val queues = new ConcurrentHashMap[String, DeltaGroupCommit[_]]()

  def forPath[R](path: String): DeltaGroupCommit[R] =
    queues.computeIfAbsent(path, _ => new DeltaGroupCommit[R]).asInstanceOf[DeltaGroupCommit[R]]
}
