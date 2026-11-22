package org.openivm.spark.telemetry

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

import scala.util.control.NonFatal

/** Driver-wide bridge between the plugin lifecycle and span emission. */
object BoundedTelemetryRuntime {
  private val active                = new AtomicReference[BoundedTelemetryQueue]()
  private val last                  = new AtomicReference[BoundedTelemetryQueue]()
  private val configurationFailures = new AtomicLong()
  private val registrationFailures  = new AtomicLong()
  private var workers               = Vector.empty[BoundedTelemetryQueue]
  private var topologyEpoch         = 0L

  /** Called after ordinary synchronous publication; never performs filesystem I/O. */
  def offer(line: String): Unit = {
    val queue = active.get()
    if (queue != null) { queue.offer(line); () }
  }

  def health: Option[BoundedTelemetryQueue.Health] = Option(last.get()).map(_.health)
  def awaitDrain(timeoutMs: Long): Boolean         = Option(active.get()).exists(_.awaitDrain(timeoutMs))
  def configurationFailureCount: Long              = configurationFailures.get()
  def registrationFailureCount: Long               = registrationFailures.get()

  private def value(f: BoundedTelemetryQueue.Health => Long): Long = health.map(f).getOrElse(0L)
  private def gauge(f: => Long): Gauge[Long]                       = new Gauge[Long] { override def getValue: Long = f }

  private val metrics: Seq[(String, Gauge[Long])] = Seq(
    "accepted"          -> gauge(value(_.accepted)),
    "published"         -> gauge(value(_.published)),
    "dropped"           -> gauge(value(h => h.droppedFull + h.droppedOversized + h.droppedClosed + h.droppedContended)),
    "dropped_full"      -> gauge(value(_.droppedFull)),
    "dropped_oversized" -> gauge(value(_.droppedOversized)),
    "dropped_closed"    -> gauge(value(_.droppedClosed)),
    "dropped_contended" -> gauge(value(_.droppedContended)),
    "export_failures"   -> gauge(value(_.exportFailures)),
    "manifest_failures" -> gauge(value(_.manifestFailures)),
    "worker_failures"   -> gauge(value(_.workerFailures)),
    "local_accepted"    -> gauge(value(_.destinations.get("local").fold(0L)(_.accepted))),
    "local_published"   -> gauge(value(_.destinations.get("local").fold(0L)(_.published))),
    "local_failures"    -> gauge(value(_.destinations.get("local").fold(0L)(_.exportFailures))),
    "local_dropped"     -> gauge(value(_.destinations.get("local").fold(0L)(_.dropped))),
    "local_manifest_failures" -> gauge(value(_.destinations.get("local").fold(0L)(_.manifestFailures))),
    "abfss_accepted"          -> gauge(value(_.destinations.get("abfss").fold(0L)(_.accepted))),
    "abfss_published"         -> gauge(value(_.destinations.get("abfss").fold(0L)(_.published))),
    "abfss_failures"          -> gauge(value(_.destinations.get("abfss").fold(0L)(_.exportFailures))),
    "abfss_dropped"           -> gauge(value(_.destinations.get("abfss").fold(0L)(_.dropped))),
    "abfss_manifest_failures" -> gauge(value(_.destinations.get("abfss").fold(0L)(_.manifestFailures))),
    "depth"                   -> gauge(value(_.queued.toLong)),
    "configuration_failures"  -> gauge(configurationFailureCount),
    "registration_failures"   -> gauge(registrationFailureCount)
  )

  /** Registers constant, label-free gauges even when ordinary OpenIVM metrics are disabled. */
  private[spark] def registerMetrics(registry: MetricRegistry): Boolean = {
    var success = true
    metrics.foreach { case (suffix, metric) =>
      val name = s"openivm.telemetry.queue.$suffix"
      try {
        val existing = registry.getMetrics.get(name)
        if (existing == null) registry.register(name, metric)
        else if (existing ne metric) throw new IllegalStateException("telemetry metric already registered")
      } catch {
        case NonFatal(_) =>
          registrationFailures.incrementAndGet()
          success = false
      }
    }
    success
  }

  /** Serialize a topology observation and the caller's guarded action with worker registration. */
  private[spark] def withSinkTopology[A](body: Long => A): A = synchronized { body(topologyEpoch) }

  /** Snapshot of actual live destinations, including workers still draining after a timeout.
    * Registration and this check are serialized; the caller must coordinate any subsequent start
    * with its protected operation if it needs the result to remain true afterwards.
    */
  private[spark] def sinksDisjoint(check: (String, Configuration) => Boolean): Boolean = synchronized {
    workers = workers.filter(_.health.workerAlive)
    try
      workers.forall { queue =>
        queue.destinations.nonEmpty && queue.destinations.forall(d => check(d.uri, new Configuration(d.hadoopConf)))
      }
    catch { case NonFatal(_) => false }
  }

  private[spark] def start(conf: => SparkConf, hadoopConf: => Configuration): Option[BoundedTelemetryQueue] =
    startWith(BoundedTelemetryQueue.configured(conf, hadoopConf))

  private[telemetry] def startWith(create: => Option[BoundedTelemetryQueue]): Option[BoundedTelemetryQueue] =
    synchronized {
      try {
        topologyEpoch = Math.addExact(topologyEpoch, 1L)
        if (active.get() != null) throw new IllegalStateException("telemetry queue is already running")
        last.set(null)
        val queue = create
        queue.foreach { q =>
          workers = workers.filter(_.health.workerAlive) :+ q
          last.set(q)
          active.set(q)
        }
        queue
      } catch {
        case NonFatal(e) =>
          configurationFailures.incrementAndGet()
          throw e
      }
    }

  private[spark] def stop(queue: BoundedTelemetryQueue, timeoutMs: Long): Boolean = {
    active.compareAndSet(queue, null)
    queue.shutdown(timeoutMs)
  }
}
