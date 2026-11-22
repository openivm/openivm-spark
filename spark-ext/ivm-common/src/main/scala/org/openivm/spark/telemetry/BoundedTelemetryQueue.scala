package org.openivm.spark.telemetry

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Locale, UUID}
import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf

import scala.util.control.NonFatal

/** Optional, best-effort span mirror. It does not replace the completed-span publisher. */
final class BoundedTelemetryQueue private[telemetry] (
    capacity: Int,
    maxChars: Int,
    writer: BoundedTelemetryQueue.Writer,
    private[telemetry] val destination: Option[BoundedTelemetryQueue.Destination] = None,
    extraDestinations: Seq[(BoundedTelemetryQueue.Writer, BoundedTelemetryQueue.Destination)] = Seq.empty
) extends SpanSink {
  import BoundedTelemetryQueue._

  require(capacity > 0 && maxChars > 0)
  private val queue            = new ConcurrentLinkedQueue[String]()
  private val permits          = new Semaphore(capacity)
  private val gate             = new ReentrantLock()
  private val accepting        = new AtomicBoolean(true)
  private val accepted         = new AtomicLong()
  private val published        = new AtomicLong()
  private val full             = new AtomicLong()
  private val oversized        = new AtomicLong()
  private val closed           = new AtomicLong()
  private val contended        = new AtomicLong()
  private val failures         = new AtomicLong()
  private val manifestFailures = new AtomicLong()
  private val workerFailures   = new AtomicLong()
  private final class SinkState(val kind: String, val writer: Writer) {
    val published        = new AtomicLong()
    val failures         = new AtomicLong()
    val manifestFailures = new AtomicLong()
    val manifestState    = new AtomicReference[String]("not_attempted")
  }
  private val sinks = new SinkState(destination.map(_.kind).getOrElse("local"), writer) +:
    extraDestinations.map { case (w, d) => new SinkState(d.kind, w) }
  private[telemetry] val destinations: Seq[Destination] = destination.toSeq ++ extraDestinations.map(_._2)
  private val startedAt                                 = Instant.now().toString
  private val json   = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  private val worker = new Thread(new Runnable { override def run(): Unit = drain() }, "openivm-telemetry-export")
  worker.setDaemon(true)
  worker.start()

  /** Never performs I/O or waits for queue space; false means the record was dropped. */
  def offer(line: String): Boolean = {
    if (line == null || line.length > maxChars) {
      oversized.incrementAndGet()
      false
    } else if (!gate.tryLock()) {
      contended.incrementAndGet()
      false
    } else {
      try {
        if (!accepting.get()) {
          closed.incrementAndGet()
          false
        } else if (!permits.tryAcquire()) {
          full.incrementAndGet()
          false
        } else {
          queue.offer(line)
          accepted.incrementAndGet()
          true
        }
      } finally gate.unlock()
    }
  }

  override def write(key: String, line: String): Unit = { offer(line); () }

  def health: Health = Health(
    accepted.get(),
    published.get(),
    full.get(),
    oversized.get(),
    closed.get(),
    contended.get(),
    failures.get(),
    manifestFailures.get(),
    queue.size(),
    accepting.get(),
    worker.isAlive,
    workerFailures.get(),
    sinks.map { sink =>
      sink.kind -> DestinationHealth(
        accepted.get(),
        sink.published.get(),
        sink.failures.get(),
        sink.manifestFailures.get(),
        full.get() + oversized.get() + closed.get() + contended.get(),
        sink.manifestState.get()
      )
    }.toMap
  )

  /** Wait for records accepted before this call to finish export, without stopping the queue. */
  def awaitDrain(timeoutMs: Long): Boolean = {
    require(timeoutMs >= 0L)
    gate.lock()
    val target =
      try accepted.get()
      finally gate.unlock()
    val started = System.nanoTime()
    val timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    while (published.get() + failures.get() < target && worker.isAlive && System.nanoTime() - started < timeout) {
      val remaining = timeout - (System.nanoTime() - started)
      if (remaining > 0L) TimeUnit.NANOSECONDS.sleep(math.min(remaining, TimeUnit.MILLISECONDS.toNanos(25L)))
    }
    published.get() + failures.get() >= target
  }

  /** Stop accepting and wait at most timeoutMs; a false return means the worker is still draining. */
  def shutdown(timeoutMs: Long): Boolean = {
    require(timeoutMs >= 0L)
    gate.lock()
    try accepting.set(false)
    finally gate.unlock()
    if (timeoutMs > 0L) worker.join(timeoutMs)
    !worker.isAlive
  }

  private def drain(): Unit = {
    try {
      while (accepting.get() || !queue.isEmpty) {
        val line = queue.poll()
        if (line == null) Thread.sleep(25L)
        else {
          permits.release()
          val rendered =
            try {
              val raw  = if (line.startsWith(LogPrefix)) line.substring(LogPrefix.length) else line
              val node = json.readTree(raw)
              if (node == null || !node.isObject) throw new IllegalArgumentException("expected a JSON object")
              Some(json.writeValueAsString(node) + "\n")
            } catch { case NonFatal(_) => None }
          var allPublished = rendered.nonEmpty
          sinks.foreach { sink =>
            try {
              sink.writer.record(rendered.getOrElse(throw new IllegalArgumentException("invalid JSON object")))
              sink.published.incrementAndGet()
            } catch {
              case NonFatal(_) =>
                sink.failures.incrementAndGet()
                allPublished = false
            }
          }
          if (allPublished) published.incrementAndGet()
          else failures.incrementAndGet()
        }
      }
    } catch {
      case NonFatal(_) =>
        gate.lock()
        try accepting.set(false)
        finally gate.unlock()
        workerFailures.incrementAndGet()
    } finally {
      sinks.foreach { sink =>
        sink.manifestState.set("writing")
        try {
          sink.writer.manifest(manifestJson())
          sink.manifestState.set("published")
        } catch {
          case NonFatal(_) =>
            sink.manifestState.set("failed")
            sink.manifestFailures.incrementAndGet()
            manifestFailures.incrementAndGet()
        }
      }
    }
  }

  private def manifestJson(): String = {
    val s    = health
    val node = json.createObjectNode()
    node.put("schema", "openivm.telemetry-export-manifest.v1")
    node.put("started_at", startedAt)
    node.put("finished_at", Instant.now().toString)
    node.put("accepted", s.accepted)
    node.put("published", s.published)
    node.put("dropped_full", s.droppedFull)
    node.put("dropped_oversized", s.droppedOversized)
    node.put("dropped_closed", s.droppedClosed)
    node.put("dropped_contended", s.droppedContended)
    node.put("export_failures", s.exportFailures)
    node.put("remaining", s.queued)
    node.put("worker_failures", s.workerFailures)
    val destinationNodes = node.putObject("destinations")
    s.destinations.foreach { case (kind, health) =>
      val entry = destinationNodes.putObject(kind)
      entry.put("accepted", health.accepted)
      entry.put("published", health.published)
      entry.put("export_failures", health.exportFailures)
      entry.put("manifest_failures", health.manifestFailures)
      entry.put("dropped", health.dropped)
      entry.put("manifest_state", health.manifestState)
    }
    json.writeValueAsString(node) + "\n"
  }
}

object BoundedTelemetryQueue {
  val EnabledKey        = "spark.openivm.telemetry.queue.enabled"
  val UriKey            = "spark.openivm.telemetry.queue.uri"
  val LocalUriKey       = "spark.openivm.telemetry.queue.localUri"
  val OneLakeUriKey     = "spark.openivm.telemetry.queue.oneLakeUri"
  val CapacityKey       = "spark.openivm.telemetry.queue.capacity"
  val MaxCharsKey       = "spark.openivm.telemetry.queue.maxChars"
  private val LogPrefix = "OPENIVM_EXECUTION_SPAN "

  final case class Health(
      accepted: Long,
      published: Long,
      droppedFull: Long,
      droppedOversized: Long,
      droppedClosed: Long,
      droppedContended: Long,
      exportFailures: Long,
      manifestFailures: Long,
      queued: Int,
      accepting: Boolean,
      workerAlive: Boolean,
      workerFailures: Long,
      destinations: Map[String, DestinationHealth]
  )

  final case class DestinationHealth(
      accepted: Long,
      published: Long,
      exportFailures: Long,
      manifestFailures: Long,
      dropped: Long,
      manifestState: String
  )

  private[telemetry] final case class Destination(uri: String, hadoopConf: Configuration) {
    def kind: String =
      if (Option(new Path(uri).toUri.getScheme).exists(_.equalsIgnoreCase("abfss"))) "abfss" else "local"
  }

  private[telemetry] trait Writer {
    def record(line: String): Unit
    def manifest(json: String): Unit
  }

  /** Resolve the exact validated candidate destinations used when registering a worker. */
  private[spark] def configuredDestinationUris(conf: SparkConf): Seq[String] = {
    val enabled = conf.getOption(EnabledKey).map(_.trim.toLowerCase(Locale.ROOT)) match {
      case None | Some("false") => false
      case Some("true")         => true
      case _                    => throw new IllegalArgumentException(s"$EnabledKey must be true or false")
    }
    if (!enabled) Seq.empty
    else {
      def read(key: String): Option[String] = conf.getOption(key).map(_.trim).filter(_.nonEmpty)
      val legacy                            = read(UriKey)
      val local                             = read(LocalUriKey)
      val oneLake                           = read(OneLakeUriKey)
      require(
        !(legacy.nonEmpty && (local.nonEmpty || oneLake.nonEmpty)),
        "legacy and explicit telemetry URIs cannot be combined"
      )
      val candidates =
        legacy.toSeq.map(_ -> "either") ++ local.toSeq.map(_ -> "local") ++ oneLake.toSeq.map(_ -> "abfss")
      require(candidates.nonEmpty, "telemetry queue destination is required")
      val paths = candidates.map { case (uri, expected) =>
        val path   = new Path(uri)
        val parsed = path.toUri
        val scheme = Option(parsed.getScheme).getOrElse("file").toLowerCase(Locale.ROOT)
        require(scheme == "file" || scheme == "abfss", "telemetry queue URI must use file or abfss")
        require(
          expected == "either" || (expected == "local" && scheme == "file") || expected == scheme,
          "telemetry destination URI has the wrong scheme"
        )
        require(
          parsed.getRawQuery == null && parsed.getRawFragment == null,
          "telemetry queue URI cannot include query or fragment"
        )
        require(
          scheme != "abfss" || Option(parsed.getAuthority).exists(_.contains("@")),
          "abfss telemetry queue URI must include filesystem and account"
        )
        path.toString
      }
      require(paths.distinct.size == paths.size, "telemetry queue destinations must be distinct")
      paths
    }
  }

  /** Construct once per driver, retain it for the driver's lifetime, and shut it down on termination. */
  def configured(conf: SparkConf, hadoopConf: Configuration): Option[BoundedTelemetryQueue] = {
    val paths = configuredDestinationUris(conf)
    if (paths.isEmpty) None
    else {
      val capacity = conf.getInt(CapacityKey, 1024)
      val maxChars = conf.getInt(MaxCharsKey, 262144)
      require(capacity > 0 && maxChars > 0, "telemetry queue limits must be positive")
      val copiedConf = new Configuration(hadoopConf)
      val configured = paths.map { uri =>
        val destination = Destination(uri, copiedConf)
        (new HadoopWriter(new Path(uri), copiedConf): Writer, destination)
      }
      Some(
        new BoundedTelemetryQueue(
          capacity,
          maxChars,
          configured.head._1,
          Some(configured.head._2),
          configured.tail
        )
      )
    }
  }

  private final class HadoopWriter(root: Path, conf: Configuration) extends Writer {
    private val run      = UUID.randomUUID().toString
    private var sequence = 0L

    override def record(line: String): Unit = {
      sequence += 1
      publish(f"$run-$sequence%020d.jsonl", line)
    }
    override def manifest(json: String): Unit = publish(s"$run-manifest.json", json)

    private def publish(name: String, content: String): Unit = {
      val fs = root.getFileSystem(conf)
      if (!fs.exists(root) && !fs.mkdirs(root)) throw new IllegalStateException("cannot create telemetry directory")
      val dest = new Path(root, name)
      val temp = new Path(root, s".$name.partial")
      try {
        val out = fs.create(temp, false)
        try out.write(content.getBytes(StandardCharsets.UTF_8))
        finally out.close()
        if (!fs.rename(temp, dest)) throw new IllegalStateException("telemetry publication failed")
      } finally {
        try fs.delete(temp, false)
        catch { case NonFatal(_) => () }
      }
    }
  }
}
