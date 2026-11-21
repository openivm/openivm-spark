package org.openivm.spark.commands

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import org.openivm.spark.common.MonotonicClockDomain
import org.openivm.spark.insights.OpenIvmInsightJson
import org.openivm.spark.telemetry.BoundedTelemetryRuntime

private[commands] final class PhaseTimeline(
    refreshId: String,
    viewName: String,
    operation: String,
    emit: String => Unit = BoundedTelemetryRuntime.offer,
    nanoTime: () => Long = () => System.nanoTime(),
    epochMillis: () => Long = () => System.currentTimeMillis()
) {
  private val sequence = new AtomicLong()
  private val stack = new ThreadLocal[List[Frame]]()

  private final class Frame(val id: Long, val startNs: Long, val startEpochMs: Long, val parent: Option[Long]) {
    var childNs = 0L
  }

  def time[A](phase: String, detail: String)(body: => A): A = {
    val ancestors = Option(stack.get()).getOrElse(Nil)
    val frame = new Frame(sequence.incrementAndGet(), nanoTime(), epochMillis(), ancestors.headOption.map(_.id))
    stack.set(frame :: ancestors)
    var outcome = "failed"
    try {
      val value = body
      outcome = "success"
      value
    } finally {
      val endNs = nanoTime()
      val elapsed = math.max(0L, endNs - frame.startNs)
      ancestors.headOption.foreach(parent => parent.childNs += elapsed)
      if (ancestors.isEmpty) stack.remove() else stack.set(ancestors)
      emit(OpenIvmInsightJson.obj(
        "event_type" -> "phase",
        "refresh_id" -> refreshId,
        "materialized_view" -> viewName,
        "operation" -> operation,
        "phase" -> phase,
        "detail" -> detail,
        "phase_id" -> frame.id,
        "parent_phase_id" -> frame.parent,
        "thread_name" -> Thread.currentThread().getName,
        "clock_domain_id" -> MonotonicClockDomain.id,
        "started_at" -> Instant.ofEpochMilli(frame.startEpochMs).toString,
        "start_monotonic_ns" -> frame.startNs,
        "inclusive_ns" -> elapsed,
        "exclusive_ns" -> math.max(0L, elapsed - frame.childNs),
        "exclusive_scope" -> "same_thread_instrumented_children",
        "outcome" -> outcome
      ))
    }
  }
}
