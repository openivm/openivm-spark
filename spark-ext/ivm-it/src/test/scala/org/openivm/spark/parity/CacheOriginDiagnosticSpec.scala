package org.openivm.spark.parity

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.common.{CacheOriginDiagnostic, FeatureGate, MvCatalog}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class CacheOriginDiagnosticSpec extends IvmParitySpecBase("cache-origin-diagnostic") with CdfMode {
  override protected def sparkMaster: String = "local[2]"
  override protected def extraSparkConf: Map[String, String] = Map(
    CacheOriginDiagnostic.ConfigKey                  -> "true",
    FeatureGate.CompileClassificationCacheEnabledKey -> "true"
  )

  private val mapper = new ObjectMapper()

  private def record(request: String): JsonNode = {
    val records = mapper.readTree(CacheOriginDiagnostic.recordsJson(request))
    records.size() shouldBe 1
    records.get(0)
  }

  private def refreshAs(view: String, request: String, owner: String): Unit = {
    spark.sparkContext.setLocalProperty("openivm.request_id", request)
    spark.sparkContext.setLocalProperty("openivm.node_id", owner)
    try refreshMv(view)
    finally {
      spark.sparkContext.setLocalProperty("openivm.request_id", null)
      spark.sparkContext.setLocalProperty("openivm.node_id", null)
    }
  }

  describe("cache origin diagnostic") {
    it("binds concurrent changed-input misses and hits, and rejects replacement and failed work") {
      Seq("a", "b").foreach { suffix =>
        sql(s"CREATE TABLE origin_src_$suffix(id INT, v INT) USING DELTA").collect()
        sql(s"INSERT INTO origin_src_$suffix VALUES (1,10)").collect()
        sql(s"CREATE MATERIALIZED VIEW origin_mv_$suffix AS SELECT id,v FROM origin_src_$suffix").collect()
      }

      def concurrent(cycle: String): Unit = {
        val executor                      = Executors.newFixedThreadPool(2)
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
        val ready                         = new CountDownLatch(2)
        val go                            = new CountDownLatch(1)
        try {
          val jobs = Seq("a", "b").map { suffix =>
            Future {
              ready.countDown()
              if (!go.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("workers did not start")
              refreshAs(s"origin_mv_$suffix", s"$cycle-$suffix", s"dbt:$suffix")
            }
          }
          ready.await(30, TimeUnit.SECONDS) shouldBe true
          go.countDown()
          Await.result(Future.sequence(jobs), 10.minutes)
        } finally executor.shutdownNow()
      }

      Seq("a", "b").foreach(s => sql(s"INSERT INTO origin_src_$s VALUES (2,20)").collect())
      concurrent("miss")
      Seq("a", "b").foreach(s => sql(s"INSERT INTO origin_src_$s VALUES (3,30)").collect())
      concurrent("hit")

      Seq("a", "b").foreach { s =>
        val miss = record(s"miss-$s")
        val hit  = record(s"hit-$s")
        miss.path("command_status").asText() shouldBe "success"
        hit.path("command_status").asText() shouldBe "success"
        miss.path("cache_hit").asBoolean() shouldBe false
        hit.path("cache_hit").asBoolean() shouldBe true
        miss.path("cache_write_succeeded").asBoolean() shouldBe true
        miss.path("dbt_node_id").asText() shouldBe s"dbt:$s"
        miss.path("application_id").asText() shouldBe hit.path("application_id").asText()
        miss.path("jvm_id").asText() shouldBe hit.path("jvm_id").asText()
        miss.path("target_table_id").asText() shouldBe hit.path("target_table_id").asText()
        miss.path("compiled_sql_sha256").asText() shouldBe hit.path("compiled_sql_sha256").asText()
        val persisted = MvCatalog.lookup(spark, TableIdentifier(s"origin_mv_$s")).get
        CacheOriginDiagnostic.sha256(persisted.properties(miss.path("cache_key").asText())) shouldBe
          miss.path("compiled_sql_sha256").asText()
        hit.path("origin_operation_id").asText() shouldBe miss.path("operation_id").asText()
        miss.path("native_invocations").size() should be > 0
        miss.path("native_invocations").get(0).path("executable_sha256").asText() shouldBe
          "7ec8dce4491fe4365aba27439d010396c8529d6551e476925b312ccb22b4389b"
        miss.path("native_invocations").get(0).path("extension_file_sha256").asText() shouldBe
          "fc5cec69e8fee7fc555895d02743c013168904b10f9af702f9b716a64fc01388"
        miss.path("native_invocations").get(0).path("ordered_native_rows").size() should be > 0
        miss.path("native_invocations").get(0).path("native_pid").asText().toLong should be > 0L
        miss.path("native_invocations").get(0).path("native_start_instant").asText() should not be empty
        miss.path("compiled_statement_sha256").size() should be > 0
        hit.path("native_invocations").size() shouldBe 0
        miss.path("rewritten_sha256").size() should be > 0
        assertMvCorrect(s"origin_mv_$s", s"SELECT id,v FROM origin_src_$s")
      }

      val oldId = record("hit-a").path("target_table_id").asText()
      sql("DROP MATERIALIZED VIEW origin_mv_a").collect()
      sql("CREATE MATERIALIZED VIEW origin_mv_a AS SELECT id,v FROM origin_src_a").collect()
      sql("INSERT INTO origin_src_a VALUES (4,40)").collect()
      refreshAs("origin_mv_a", "replacement-a", "dbt:a")
      val replacement = record("replacement-a")
      replacement.path("target_table_id").asText() should not be oldId
      replacement.path("cache_hit").asBoolean() shouldBe false
      replacement.has("origin_operation_id") shouldBe false

      val metadata = MvCatalog.lookup(spark, TableIdentifier("origin_mv_b")).get
      val poisoned = metadata.properties.map {
        case (key, value) if key.startsWith("_ivm_compile_cache:") && key.endsWith(":sql") =>
          key -> value.replace("openivm_delta_origin_src_b", "openivm_delta_origin_missing")
        case entry => entry
      }
      MvCatalog.updateProperties(spark, metadata.name, poisoned)
      sql("INSERT INTO origin_src_b VALUES (4,40)").collect()
      intercept[Exception](refreshAs("origin_mv_b", "failure-b", "dbt:b"))
      val failed = record("failure-b")
      failed.path("command_status").asText() shouldBe "error"
      failed.path("observation_status").asText() shouldBe "incomplete"
      failed.has("origin_operation_id") shouldBe false

      sql("CREATE TABLE origin_src_c(id INT, v INT) USING DELTA").collect()
      sql("INSERT INTO origin_src_c VALUES (1,10)").collect()
      sql("CREATE MATERIALIZED VIEW origin_mv_c AS SELECT id,v FROM origin_src_c").collect()
      sql("INSERT INTO origin_src_c VALUES (2,20)").collect()
      spark.conf.set(CacheOriginDiagnostic.ConfigKey, "false")
      refreshAs("origin_mv_c", "off-c", "dbt:c")
      mapper.readTree(CacheOriginDiagnostic.recordsJson("off-c")).size() shouldBe 0
      assertMvCorrect("origin_mv_c", "SELECT id,v FROM origin_src_c")
      spark.conf.set(CacheOriginDiagnostic.ConfigKey, "true")
      sql("INSERT INTO origin_src_c VALUES (3,30)").collect()
      refreshAs("origin_mv_c", "hit-without-origin-c", "dbt:c")
      val missing = record("hit-without-origin-c")
      missing.path("cache_hit").asBoolean() shouldBe true
      missing.has("origin_operation_id") shouldBe false
      missing.path("observation_status").asText() shouldBe "incomplete"
      assertMvCorrect("origin_mv_c", "SELECT id,v FROM origin_src_c")
      val evidence = mapper.createArrayNode()
      Seq("miss-a", "miss-b", "hit-a", "hit-b", "replacement-a", "failure-b", "hit-without-origin-c").foreach {
        request =>
          evidence.add(record(request))
      }
      Files.write(
        Paths.get("target", "cache-origin-synthetic-observations.json"),
        mapper.writeValueAsString(evidence).getBytes(StandardCharsets.UTF_8)
      )
    }
  }
}
