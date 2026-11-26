package org.openivm.spark.compiler

import java.nio.file.Files
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CompilerRuntimeProbeSpec extends AnyFunSuite with Matchers {
  private def withCli(body: String)(check: OpenIvmCompiler => Unit): Unit = {
    val dir = Files.createTempDirectory("compiler-probe-test")
    val cli = dir.resolve("duckdb")
    Files.write(cli, ("#!/bin/sh\nscript=$(cat)\n" + body + "\n").getBytes("UTF-8"))
    cli.toFile.setExecutable(true) shouldBe true
    val compiler = OpenIvmCompiler.build(extensionPath = cli.toString, cliPath = cli.toString)
    try check(compiler)
    finally {
      compiler.close()
      Files.deleteIfExists(dir.resolve("probes"))
      Files.deleteIfExists(cli)
      Files.deleteIfExists(dir)
    }
  }

  test("loader failure is fatal and preserves stderr instead of enabling full refresh") {
    withCli("echo 'libstdc++.so.6: GLIBCXX_3.4.32 not found' >&2; exit 1") { compiler =>
      val error = intercept[IllegalStateException](compiler.verifyRuntime())
      error should not be a[OpenIvmCompileException]
      error.getMessage should include("GLIBCXX_3.4.32")
      error.getMessage should include("refusing full-refresh fallback")
    }
  }

  test("a compiler that returns an incremental projection passes the probe") {
    withCli(
      """echo '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT id FROM delta_source"}'"""
    ) {
      _.verifyRuntime()
    }
  }

  test("the CLI permits loading the bundled unsigned extension") {
    withCli(
      """case " $* " in
        |  *" -unsigned "*) echo '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT id FROM delta_source"}' ;;
        |  *) echo 'unsigned extensions disabled' >&2; exit 1 ;;
        |esac""".stripMargin
    ) {
      _.verifyRuntime()
    }
  }

  test("a compiler returning full refresh for the projection fails the probe") {
    withCli("""echo '{"refresh_type":4,"refresh_type_name":"FULL_REFRESH","sql":"SELECT id FROM source"}'""") {
      compiler =>
        intercept[IllegalStateException](compiler.verifyRuntime()).getMessage should include("incremental projection")
    }
  }

  private val CountProbes =
    """case "$script" in *openivm_runtime_probe*) echo probe >> "$(dirname "$0")/probes" ;; esac"""
  private val Incremental =
    """echo '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT id FROM s"}'"""

  private def probes(compiler: OpenIvmCompiler): Int = {
    val file = new java.io.File(compiler.cliPath).toPath.resolveSibling("probes")
    if (Files.exists(file)) Files.readAllLines(file).size else 0
  }

  private def request(view: String) =
    CompileRequest(
      viewName = view,
      viewSql = "SELECT id FROM s",
      sources = Map("s" -> StructType(Seq(StructField("id", IntegerType))))
    )

  test("deferred verification skips the probe while compiles stay incremental") {
    withCli(s"$CountProbes\n$Incremental") { compiler =>
      compiler.deferRuntimeVerification()
      compiler.compile(request("v1")).refreshTypeName shouldBe "SIMPLE_PROJECTION"
      compiler.compile(request("v2")).refreshTypeName shouldBe "SIMPLE_PROJECTION"
      probes(compiler) shouldBe 0
    }
  }

  test("deferred verification still refuses the fallback of a broken runtime") {
    withCli("echo 'libstdc++.so.6: GLIBCXX_3.4.32 not found' >&2; exit 1") { compiler =>
      compiler.deferRuntimeVerification()
      val error = intercept[IllegalStateException](compiler.compile(request("v1")))
      error.getMessage should include("refusing full-refresh fallback")
      error.getSuppressed.toSeq.map(_.getClass) shouldBe Seq(classOf[OpenIvmCompileException])
    }
  }

  test("deferred verification probes a healthy runtime once and keeps query-specific outcomes") {
    withCli(
      s"""$CountProbes
         |case "$$script" in
         |  *openivm_runtime_probe*) $Incremental ;;
         |  *v_fails*) echo 'Catalog Error: Scalar Function spark_only does not exist' >&2; exit 1 ;;
         |  *) echo '{"refresh_type":4,"refresh_type_name":"FULL_REFRESH","sql":"SELECT id FROM s"}' ;;
         |esac""".stripMargin
    ) { compiler =>
      compiler.deferRuntimeVerification()
      intercept[OpenIvmCompileException](compiler.compile(request("v_fails"))).getMessage should include("spark_only")
      compiler.compile(request("v_full")).refreshTypeName shouldBe "FULL_REFRESH"
      probes(compiler) shouldBe 1
    }
  }
}
