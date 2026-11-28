package org.openivm.spark.compiler

import java.io.{ByteArrayInputStream, IOException, InputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class CompilerTraceSpec extends AnyFlatSpec with Matchers {
  private val validReport =
    "openivm-compiler-trace-v1\nnative_launch|1000|42|success\nworker_total|1000|50|success\nend\n"

  "Compiler tracing" should "be disabled unless explicitly enabled" in {
    val events   = ArrayBuffer.empty[String]
    var failures = 0
    val trace = new CompilerTrace(
      Some(CompileTraceContext("op", None, "mv")),
      line => { events += line; () },
      () => failures += 1
    )
    trace.time("phase", 1) { 42 } shouldBe 42
    trace.readWorkerOutput(
      new InputStream {
        override def read(): Int      = throw new IOException("sensitive")
        override def available(): Int = throw new IOException("sensitive")
      },
      1
    )
    trace.workerOutput(Array.emptyByteArray, 1)
    trace.workerMissing()
    events shouldBe empty
    failures shouldBe 0
  }

  it should "count missing, truncated, malformed, oversized and unreadable reports" in {
    val events   = ArrayBuffer.empty[String]
    var failures = 0
    val trace = new CompilerTrace(
      Some(CompileTraceContext("op", None, "mv", enabled = true)),
      line => { events += line; () },
      () => failures += 1
    )
    val invalid = Seq(
      "",
      "not-a-trace\n",
      validReport.stripSuffix("end\n"),
      validReport.replace("native_launch", "unknown"),
      validReport.replace("|42|", "|-1|"),
      validReport.replace("|42|", "|nan|"),
      validReport.replace("worker_total", "worker_output"),
      validReport.replace("success", "unknown")
    )
    invalid.foreach(value => trace.workerOutput(value.getBytes("UTF-8"), 1))
    trace.readWorkerOutput(new ByteArrayInputStream(Array.fill[Byte](CompilerTrace.MaxWorkerBytes + 1)(0)), 1)
    trace.readWorkerOutput(
      new InputStream {
        override def read(): Int      = throw new IOException("sensitive")
        override def available(): Int = throw new IOException("sensitive")
      },
      1
    )
    trace.readWorkerOutput(
      new InputStream {
        override def read(): Int      = throw new IOException("sensitive")
        override def available(): Int = 1
      },
      1
    )
    trace.readWorkerOutput(
      new ByteArrayInputStream(validReport.getBytes("UTF-8")) {
        override def available(): Int = super.available() + 1
      },
      1
    )
    trace.workerMissing()
    failures shouldBe invalid.size + 5
    events shouldBe empty
  }

  it should "count emission failures without changing successful or failed body outcomes" in {
    var failures = 0
    val trace = new CompilerTrace(
      Some(CompileTraceContext("op", None, "mv", enabled = true)),
      _ => throw new IllegalStateException("sensitive"),
      () => failures += 1
    )
    trace.time("success", 1) { 42 } shouldBe 42
    val original = new IllegalStateException("body")
    val caught   = the[IllegalStateException] thrownBy trace.time("failure", 1) { throw original }
    caught shouldBe original
    failures shouldBe 2
  }

  it should "rate-limit a constant warning while counting every failure" in {
    var now      = 10L
    val warnings = ArrayBuffer.empty[String]
    val reporter = new CompileTraceFailureReporter(
      () => now,
      message => { warnings += message; () },
      100L
    )
    val trace = new CompilerTrace(
      Some(CompileTraceContext("secret-sql", None, "secret-path", enabled = true)),
      _ => throw new IllegalStateException("secret-exception"),
      () => reporter.recordFailure()
    )
    trace.workerOutput(Array.emptyByteArray, 1)
    trace.time("phase", 1) { 42 } shouldBe 42
    now = 110L
    trace.workerMissing()
    reporter.failureCount shouldBe 3L
    warnings.toSeq shouldBe Seq(CompileTraceDiagnostics.WarningMessage, CompileTraceDiagnostics.WarningMessage)
    warnings.mkString should not include "secret"
  }

  it should "not mask compilation if warning emission fails" in {
    val reporter = new CompileTraceFailureReporter(
      () => 1L,
      _ => throw new IllegalStateException("sensitive"),
      100L
    )
    val trace = new CompilerTrace(
      Some(CompileTraceContext("op", None, "mv", enabled = true)),
      _ => throw new IllegalStateException("sensitive"),
      () => reporter.recordFailure()
    )
    trace.time("success", 1) { 42 } shouldBe 42
    val original = new IllegalStateException("body")
    val caught   = the[IllegalStateException] thrownBy trace.time("failure", 1) { throw original }
    caught shouldBe original
    reporter.failureCount shouldBe 2L
  }

  it should "return helper durations without assigning driver monotonic timestamps" in {
    val events   = ArrayBuffer.empty[String]
    var failures = 0
    val trace = new CompilerTrace(
      Some(CompileTraceContext("op", Some("req"), "mv", enabled = true)),
      line => { events += line; () },
      () => failures += 1
    )
    trace.readWorkerOutput(new ByteArrayInputStream(validReport.getBytes("UTF-8")), 2)
    val row = new ObjectMapper().readTree(events.head)
    row.path("attempt").asInt() shouldBe 2
    row.path("start_utc").asText() shouldBe "1970-01-01T00:00:01Z"
    row.path("duration_ns").asLong() shouldBe 42L
    row.path("clock_domain").asText() shouldBe "helper"
    row.has("start_monotonic_ns") shouldBe false
    failures shouldBe 0
  }
}
