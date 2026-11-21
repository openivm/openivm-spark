package org.openivm.spark.common

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.UUID
import scala.collection.mutable
import scala.util.control.NonFatal

/** Request-owned diagnostic observations. This is not an attribution verdict. */
object CacheOriginDiagnostic extends Logging {
  private val mapper = new ObjectMapper()
  private val completed = mutable.LinkedHashMap.empty[String, (String, String)]
  private val active = mutable.Map.empty[String, mutable.Set[String]]
  private val ambiguous = mutable.Set.empty[String]
  private val origins = mutable.LinkedHashMap.empty[String, String]
  private val limit = 4096
  val ConfigKey = "spark.openivm.diagnostics.cacheOrigin.enabled"

  def observe(origin: Option[Recorder])(f: Recorder => Unit): Unit = origin.foreach { recorder =>
    try f(recorder)
    catch {
      case NonFatal(e) =>
        logWarning("Cache-origin diagnostic observation failed", e)
        try recorder.problem(s"observer_error:${e.getClass.getName}")
        catch { case NonFatal(nested) => logWarning("Could not mark cache-origin observation incomplete", nested) }
    }
  }

  def sha256(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_)).mkString

  def fileSha256(path: Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val input = Files.newInputStream(path)
    val buffer = new Array[Byte](65536)
    try {
      var n = input.read(buffer)
      while (n >= 0) {
        if (n > 0) digest.update(buffer, 0, n)
        n = input.read(buffer)
      }
    } finally input.close()
    digest.digest().map("%02x".format(_)).mkString
  }

  def start(spark: SparkSession, target: String): Option[Recorder] = try {
    if (!spark.conf.getOption(ConfigKey).exists(_.equalsIgnoreCase("true"))) None
    else {
      val sc = spark.sparkContext
      val recorder = new Recorder(
        UUID.randomUUID().toString,
        sc.applicationId,
        target,
        Option(sc.getLocalProperty("openivm.request_id")).filter(_.trim.nonEmpty),
        Option(sc.getLocalProperty("openivm.node_id"))
          .orElse(Option(sc.getLocalProperty("spark.jobGroup.id")))
          .filter(_.trim.nonEmpty)
      )
      synchronized {
        val members = active.getOrElseUpdate(recorder.ownerKey, mutable.Set.empty[String])
        if (members.nonEmpty) {
          ambiguous ++= members
          ambiguous += recorder.operationId
        }
  } catch {
        case NonFatal(e) =>
          logWarning("Could not start cache-origin diagnostic", e)
          None
  }
        members += recorder.operationId
      }
      Some(recorder)
    }

  /** Returns all terminal records for an exact request. Multiple records are ambiguous. */
  def recordsJson(requestId: String): String = synchronized {
    val records = new java.util.ArrayList[com.fasterxml.jackson.databind.JsonNode]()
    completed.valuesIterator.filter(_._1 == requestId).foreach { case (_, json) =>
      records.add(mapper.readTree(json))
    }
    mapper.writeValueAsString(records)
  }

  private def publish(id: String, request: String, json: String): Unit = synchronized {
    completed.put(id, request -> json)
    while (completed.size > limit) completed.remove(completed.head._1)
  }

  final class Recorder private[CacheOriginDiagnostic] (
      val operationId: String,
      applicationId: String,
      target: String,
      requestId: Option[String],
      owner: Option[String]
  ) {
    private[CacheOriginDiagnostic] val ownerKey: String =
      Seq(applicationId, requestId.getOrElse(""), owner.getOrElse(""))
        .map(s => s"${s.length}:$s").mkString
    private val fields = mapper.createObjectNode()
    private val invocations = mapper.createArrayNode()
    private val executions = mapper.createArrayNode()
    private val problems = mapper.createArrayNode()
    private var boundIdentity = Option.empty[(String, String)]
    private var cacheSignature = Option.empty[String]
    private var cacheHit = false
    private var persisted = false
    private var terminal = false
    fields.put("operation_id", operationId)
    fields.put("application_id", applicationId)
    fields.put("target", target)
    requestId.foreach(fields.put("request_id", _))
    owner.foreach(fields.put("dbt_node_id", _))
    if (requestId.isEmpty) problems.add("missing_request_id")
    if (owner.isEmpty) problems.add("missing_dbt_owner")

    def problem(value: String): Unit = synchronized { problems.add(value); () }
    def profile(id: String): Unit = synchronized { fields.put("profile_id", id); () }

    def bind(tableId: String, pathHash: String, queryHash: String, schema: String): Unit = synchronized {
      boundIdentity = Some(pathHash -> tableId)
      fields.put("target_table_id", tableId)
      fields.put("target_path_sha256", pathHash)
      fields.put("query_sha256", queryHash)
      fields.put("source_schema_fingerprint", schema)
    }

    def verifyIdentity(identity: Option[(String, String)]): Unit = synchronized {
      if (identity.map { case (path, id) => sha256(path) -> id } != boundIdentity)
        problems.add("target_identity_changed_or_unreadable")
    }

    def native(
        inputHash: String,
        stdoutHash: String,
        executableHash: String,
        extensionHash: String,
        rows: Seq[(Int, String)]
    ): Unit =
      synchronized {
        val item = invocations.addObject()
        item.put("input_sha256", inputHash)
        item.put("stdout_sha256", stdoutHash)
        item.put("executable_sha256", executableHash)
        item.put("extension_file_sha256", extensionHash)
        val ordered = item.putArray("ordered_native_rows")
        rows.foreach { case (ordinal, digest) =>
          val row = ordered.addObject()
          row.put("stmt_order", ordinal)
          row.put("sql_sha256", digest)
        }
      }

    def cache(hit: Boolean, tier: String, key: String, code: String, classification: String, sql: String, writeSucceeded: Boolean): Unit =
      synchronized {
        cacheHit = hit
        persisted = writeSucceeded
        val parts = Seq(applicationId, target, fields.path("target_table_id").asText(),
          fields.path("target_path_sha256").asText(), fields.path("query_sha256").asText(),
          fields.path("source_schema_fingerprint").asText(), tier, key, code, classification, sha256(sql))
        cacheSignature = Some(sha256(parts.map(s => s"${s.length}:$s").mkString))
        fields.put("tier", tier)
        fields.put("cache_key", key)
        fields.put("compiled_sql_sha256", sha256(sql))
        fields.put("classification_code", code)
        fields.put("classification", classification)
        fields.put("cache_hit", hit)
        fields.put("cache_write_succeeded", writeSucceeded)
        if (sql.isEmpty) problems.add("empty_compiled_program")
        if (!hit && !writeSucceeded) problems.add("cache_write_not_persisted")
        if (!hit && invocations.isEmpty) problems.add("native_origin_missing")
      }

    def rewritten(statements: Seq[String]): Unit = synchronized {
      val plan = fields.putArray("rewritten_sha256")
      statements.foreach(s => plan.add(sha256(s)))
    }

    def executed(index: Int, sql: String, succeeded: Boolean): Unit = synchronized {
      val item = executions.addObject()
      item.put("statement_index", index)
      item.put("sql_sha256", sha256(sql))
      item.put("succeeded", succeeded)
    }

    def finish(success: Boolean): Unit = synchronized {
      if (!terminal) {
        terminal = true
        fields.put("command_status", if (success) "success" else "error")
        if (boundIdentity.isEmpty) problems.add("target_identity_unverified")
        if (cacheSignature.isEmpty) problems.add("cache_selection_not_observed")
        if (!success) problems.add("command_failed")
        CacheOriginDiagnostic.synchronized {
          if (ambiguous.remove(operationId)) problems.add("concurrent_owner_ambiguous")
          active.get(ownerKey).foreach { members =>
            members -= operationId
            if (members.isEmpty) active.remove(ownerKey)
          }
          cacheSignature.foreach { signature =>
            if (cacheHit) {
              origins.get(signature) match {
                case Some(id) => fields.put("origin_operation_id", id)
                case None     => problems.add("matching_miss_provenance_missing")
              }
            } else if (persisted && success && problems.isEmpty) {
              origins.put(signature, operationId)
              while (origins.size > limit) origins.remove(origins.head._1)
            }
          }
        }
        fields.put("execution_coverage", "common_sql_dispatch_only")
        problems.add("execution_coverage_unverified")
        fields.put("observation_status", if (problems.isEmpty) "observed" else "incomplete")
        fields.set[com.fasterxml.jackson.databind.JsonNode]("native_invocations", invocations)
        fields.set[com.fasterxml.jackson.databind.JsonNode]("executions", executions)
        fields.set[com.fasterxml.jackson.databind.JsonNode]("problems", problems)
        try publish(operationId, requestId.getOrElse(""), mapper.writeValueAsString(fields))
        catch { case NonFatal(e) => logWarning("Failed to publish cache-origin diagnostic", e) }
      }
    }
  }
}
