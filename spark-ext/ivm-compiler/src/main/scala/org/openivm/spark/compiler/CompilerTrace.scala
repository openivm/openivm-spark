package org.openivm.spark.compiler

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import org.openivm.spark.insights.OpenIvmInsightJson
import org.openivm.spark.telemetry.BoundedTelemetryRuntime

import scala.util.control.NonFatal

/** Explicit driver-side correlation; the worker receives no identity or SQL. */
final case class CompileTraceContext(
    operationId: String,
    requestId: Option[String],
    materializedView: String,
    enabled: Boolean = false,
    sqlExecutionId: Option[String] = None
)

/** Process-wide count of trace failures, independent of optional Spark metrics. */
object CompileTraceDiagnostics {
  private[compiler] val WarningMessage =
    "OpenIVM compiler trace diagnostics were dropped; compilation is unaffected."
  private lazy val logger = org.slf4j.LoggerFactory.getLogger("org.openivm.spark.compiler.CompileTraceDiagnostics")
  private val reporter = new CompileTraceFailureReporter(
    () => System.nanoTime(),
    message => logger.warn(message),
    TimeUnit.MINUTES.toNanos(1)
  )

  def failureCount: Long = reporter.failureCount
  private[compiler] def recordFailure(): Unit = reporter.recordFailure()
}

private[compiler] final class CompileTraceFailureReporter(
    clock: () => Long,
    warn: String => Unit,
    intervalNanos: Long
) {
  private val failures = new AtomicLong()
  private val lastWarning = new AtomicReference[java.lang.Long]()

  def failureCount: Long = failures.get()

  def recordFailure(): Unit = {
    failures.incrementAndGet()
    try {
      val now = clock()
      val previous = lastWarning.get()
      if ((previous eq null) || now - previous.longValue() >= intervalNanos) {
        if (lastWarning.compareAndSet(previous, java.lang.Long.valueOf(now)))
          warn(CompileTraceDiagnostics.WarningMessage)
      }
    } catch { case NonFatal(_) => () }
  }
}

private[compiler] final class CompilerTrace(
    context: Option[CompileTraceContext],
    emit: String => Unit = BoundedTelemetryRuntime.offer,
    onFailure: () => Unit = () => CompileTraceDiagnostics.recordFailure()
) {
  val enabled: Boolean = context.exists(_.enabled)

  def time[A](phase: String, attempt: Int)(body: => A): A = {
    if (!enabled) return body
    val utc = Instant.now().toString
    val start = System.nanoTime()
    var success = false
    try {
      val result = body
      success = true
      result
    } finally record(phase, attempt, utc, System.nanoTime() - start, if (success) "success" else "failed", "driver")
  }

  private def failure(): Unit =
    try onFailure()
    catch { case NonFatal(_) => CompileTraceDiagnostics.recordFailure() }

  def workerMissing(): Unit = if (enabled) failure()

  def readWorkerOutput(input: InputStream, attempt: Int): Unit = {
    if (!enabled) return
    try {
      val available = input.available()
      if (available > CompilerTrace.MaxWorkerBytes) failure()
      else {
        val bytes = input.readNBytes(available)
        if (bytes.length != available) failure()
        else workerOutput(bytes, attempt)
      }
    } catch { case NonFatal(_) => failure() }
  }

  def workerOutput(bytes: Array[Byte], attempt: Int): Unit = {
    if (!enabled) return
    try {
      val text = new String(bytes, StandardCharsets.UTF_8)
      val lines = text.split("\n", -1).toVector
      require(lines.length >= 4 && lines.head == "openivm-compiler-trace-v1" &&
        lines(lines.length - 2) == "end" && lines.last.isEmpty)
      val entries = lines.slice(1, lines.length - 2).map { line =>
        val fields = line.split("\\|", -1)
        require(fields.length == 4 && CompilerTrace.workerPhases.contains(fields(0)) &&
          Set("success", "failed").contains(fields(3)))
        val epochMs = fields(1).toLong
        val duration = fields(2).toLong
        require(duration >= 0L)
        (fields(0), Instant.ofEpochMilli(epochMs).toString, duration, fields(3))
      }
      require(entries.map(_._1).distinct.size == entries.size && entries.exists(_._1 == "worker_total"))
      entries.foreach { case (phase, utc, duration, status) =>
        record(phase, attempt, utc, duration, status, "helper")
      }
    } catch { case NonFatal(_) => failure() }
  }

  private def record(phase: String, attempt: Int, utc: String, duration: Long, status: String, domain: String): Unit =
    try {
      context.filter(_.enabled).foreach { c =>
        emit(OpenIvmInsightJson.obj(
          "schema" -> "openivm.compiler-phase.v1",
          "record_type" -> "compiler_phase",
          "operation_id" -> c.operationId,
          "request_id" -> c.requestId,
          "materialized_view" -> c.materializedView,
          "sql_execution_id" -> c.sqlExecutionId,
          "phase" -> phase,
          "attempt" -> attempt,
          "start_utc" -> utc,
          "duration_ns" -> math.max(0L, duration),
          "status" -> status,
          "clock_domain" -> domain
        ))
      }
    } catch { case NonFatal(_) => failure() }
}

private[compiler] object CompilerTrace {
  val MaxWorkerBytes = 4096
  val workerPhases: Set[String] = Set("worker_total", "native_launch", "native_lifecycle", "worker_output", "worker_cleanup")
}
