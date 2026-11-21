package org.openivm.spark.parity

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{AddFile, CommitInfo, RemoveFile, SetTransaction}
import org.apache.spark.sql.openivm.SparkSessionAccess
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

import scala.util.control.NonFatal

class CdfTxnPrerequisiteCanarySpec extends IvmParitySpecBase("cdf-txn-prerequisite") with CdfMode {
  private val mapper = new ObjectMapper()
  private val runId = UUID.randomUUID().toString
  private lazy val evidenceDir = new File(s"target/cdf-txn-canary-$runId").getAbsoluteFile
  private val appKey = "spark.databricks.delta.write.txnAppId"
  private val versionKey = "spark.databricks.delta.write.txnVersion"
  private val tagKey = "spark.databricks.delta.commitInfo.userMetadata"

  private def table(name: String): String = new File(evidenceDir, name).getAbsolutePath
  private def target(path: String): String = s"delta.`$path`"
  private def log(session: SparkSession, path: String): DeltaLog = DeltaLog.forTable(session, path)

  private def active[A](session: SparkSession)(f: => A): A = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(session)
    try f
    finally previous match {
      case Some(value) => SparkSession.setActiveSession(value)
      case None        => SparkSession.clearActiveSession()
    }
  }

  private def setTxn(session: SparkSession, app: String, version: Long, tag: String): Unit = {
    session.conf.set(appKey, app)
    session.conf.set(versionKey, version.toString)
    session.conf.set(tagKey, tag)
  }

  private def create(path: String): Unit = {
    spark.sql(s"CREATE TABLE ${target(path)} (id INT, total BIGINT) USING DELTA").collect()
    spark.sql(s"INSERT INTO ${target(path)} VALUES (1, 10), (2, 20)").collect()
  }

  private def assertRows(session: SparkSession, path: String, expected: String): Unit = {
    val actual = session.read.format("delta").load(path).select("id", "total")
    val wanted: DataFrame = session.sql(expected)
    assert(actual.exceptAll(wanted).count() == 0L)
    assert(wanted.exceptAll(actual).count() == 0L)
  }

  private final class DelayedPlan(val tableVersion: Long, val usedSeq: Long, val higherSeq: Long)

  private def record(
      label: String,
      session: SparkSession,
      path: String,
      app: String,
      result: String,
      plan: Option[DelayedPlan] = None
  ): Unit = {
    val delta = log(session, path)
    val snap = delta.update()
    val root = mapper.createObjectNode()
    root.put("canary", label)
    root.put("result", result)
    root.put("spark", spark.version)
    root.put("deltaJar", new File(classOf[DeltaLog].getProtectionDomain.getCodeSource.getLocation.toURI).getName)
    root.put("runId", runId)
    root.put("target", path)
    root.put("appId", app)
    root.put("tableVersion", snap.version)
    root.put("isolationLevelProperty", snap.metadata.configuration.getOrElse("delta.isolationLevel", "<default>"))
    root.put("txnVersion", snap.transactions.getOrElse(app, -1L))
    plan.foreach { captured =>
      val intent = root.putObject("delayedPlan")
      intent.put("tableVersion", captured.tableVersion)
      intent.put("usedSeq", captured.usedSeq)
      intent.put("higherSeq", captured.higherSeq)
    }
    val rows = root.putArray("rows")
    session.read.format("delta").load(path).orderBy("id", "total").collect().foreach { row =>
      val entry = rows.addArray()
      entry.add(row.getInt(0))
      entry.add(row.getLong(1))
    }
    val commits = root.putArray("commits")
    delta.getChanges(0L, failOnDataLoss = true).takeWhile(_._1 <= snap.version).foreach { case (version, actions) =>
      val entry = commits.addObject()
      entry.put("version", version)
      val txns = entry.putArray("txns")
      actions.collect { case t: SetTransaction => t }.foreach { t =>
        val txn = txns.addObject()
        txn.put("appId", t.appId)
        txn.put("version", t.version)
      }
      val infos = entry.putArray("commitInfo")
      actions.collect { case info: CommitInfo => info }.foreach { info =>
        val metadata = infos.addObject()
        metadata.put("operation", info.operation)
        info.userMetadata.foreach(value => metadata.put("userMetadata", value))
      }
      entry.put("adds", actions.count(_.isInstanceOf[AddFile]))
      entry.put("removes", actions.count(_.isInstanceOf[RemoveFile]))
    }
    Files.createDirectories(evidenceDir.toPath)
    Files.write(
      new File(evidenceDir, "results.jsonl").toPath,
      (mapper.writeValueAsString(root) + "\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
  }

  private def observe[A](label: String, session: SparkSession, path: String, app: String)(f: => A): A = {
    try {
      val value = f
      record(label, session, path, app, "completed")
      value
    } catch {
      case NonFatal(error) =>
        record(label, session, path, app, s"${error.getClass.getName}: ${Option(error.getMessage).getOrElse("")}")
        throw error
    }
  }

  private def merge(session: SparkSession, path: String, increment: Long): Unit = {
    session.sql(
      s"""MERGE INTO ${target(path)} AS d
         |USING (SELECT 1 AS id, CAST($increment AS BIGINT) AS change) AS s
         |ON d.id = s.id
         |WHEN MATCHED THEN UPDATE SET d.total = d.total + s.change
         |WHEN NOT MATCHED THEN INSERT (id, total) VALUES (s.id, s.change)""".stripMargin
    ).collect()
  }

  private def assertCommit(session: SparkSession, path: String, version: Long, app: String, seq: Long, tag: String): Unit = {
    val actions = log(session, path).getChanges(version, failOnDataLoss = true).next()._2
    assert(actions.collect { case t: SetTransaction if t.appId == app => t.version }.toVector == Vector(seq))
    assert(actions.collect { case c: CommitInfo => c.userMetadata }.flatten.toVector == Vector(tag))
  }

  describe("bounded Delta transaction prerequisites") {
    it("records MERGE retry and a no-op final DELETE") {
      val path = table("merge")
      create(path)
      val session = SparkSessionAccess.cloneSession(spark)
      val app = s"canary-merge-$runId"
      val tag = "canary-c1-c3-v7"
      active(session) {
        val before = log(session, path).update().version
        setTxn(session, app, 7L, tag)
        observe("C1-C3-first", session, path, app) { merge(session, path, 5L) }
        val committed = log(session, path).update().version
        assert(committed == before + 1L)
        assertCommit(session, path, committed, app, 7L, tag)
        assertRows(session, path, "SELECT 1 AS id, CAST(15 AS BIGINT) AS total UNION ALL SELECT 2, CAST(20 AS BIGINT)")
        val historyTag = session.sql(s"DESCRIBE HISTORY ${target(path)}").where(s"version = $committed").select("userMetadata").head().getString(0)
        assert(historyTag == tag)
        setTxn(session, app, 7L, "retry-must-not-commit")
        observe("C1-retry", session, path, app) { merge(session, path, 5L) }
        assert(log(session, path).update().version == committed)
        assert(log(session, path).update().transactions.get(app).contains(7L))
        assertRows(session, path, "SELECT 1 AS id, CAST(15 AS BIGINT) AS total UNION ALL SELECT 2, CAST(20 AS BIGINT)")
        setTxn(session, app, 8L, "canary-c8-delete")
        observe("C8-no-op-delete", session, path, app) {
          session.sql(s"DELETE FROM ${target(path)} WHERE id = 999").collect()
        }
        assert(log(session, path).update().version == committed)
        assert(log(session, path).update().transactions.get(app).contains(7L))
        assertRows(session, path, "SELECT 1 AS id, CAST(15 AS BIGINT) AS total UNION ALL SELECT 2, CAST(20 AS BIGINT)")
      }
    }

    it("records an internal marker and overlapping same-appId transactions") {
      val path = table("marker")
      create(path)
      val session = SparkSessionAccess.cloneSession(spark)
      val app = s"canary-marker-$runId"
      active(session) {
        val delta = log(session, path)
        val initial = delta.update().version
        session.conf.set(tagKey, "canary-c10-marker")
        val first = delta.startTransaction()
        assert(first.txnVersion(app) == -1L)
        observe("C10-marker", session, path, app) {
          first.commit(Seq(SetTransaction(app, 1L, Some(System.currentTimeMillis()))), DeltaOperations.ManualUpdate)
        }
        assert(delta.update().version == initial + 1L)
        assertCommit(session, path, initial + 1L, app, 1L, "canary-c10-marker")
        val markerActions = delta.getChanges(initial + 1L, failOnDataLoss = true).next()._2
        assert(!markerActions.exists(a => a.isInstanceOf[AddFile] || a.isInstanceOf[RemoveFile]))
        assertRows(session, path, "SELECT 1 AS id, CAST(10 AS BIGINT) AS total UNION ALL SELECT 2, CAST(20 AS BIGINT)")
        val stale = delta.startTransaction()
        assert(stale.txnVersion(app) == 1L)
        val winner = delta.startTransaction()
        assert(winner.txnVersion(app) == 1L)
        session.conf.set(tagKey, "canary-c10-winner")
        observe("C10-winner", session, path, app) {
          winner.commit(Seq(SetTransaction(app, 2L, Some(System.currentTimeMillis()))), DeltaOperations.ManualUpdate)
        }
        session.conf.set(tagKey, "canary-c10-stale")
        val failure = try {
          stale.commit(Seq(SetTransaction(app, 3L, Some(System.currentTimeMillis()))), DeltaOperations.ManualUpdate)
          None
        } catch { case NonFatal(error) => Some(error) }
        record("C10-overlap", session, path, app, failure.map(e => s"${e.getClass.getName}: ${e.getMessage}").getOrElse("unexpected-success"))
        assert(failure.exists(_.getClass.getName.contains("ConcurrentTransactionException")))
        assert(delta.update().version == initial + 2L)
        assert(delta.update().transactions.get(app).contains(2L))
        assertCommit(session, path, initial + 2L, app, 2L, "canary-c10-winner")
      }
    }

    it("records delayed used and higher planned sequences after advancement") {
      val path = table("ordering")
      create(path)
      val advanced = SparkSessionAccess.cloneSession(spark)
      val delayed = SparkSessionAccess.cloneSession(spark)
      val app = s"canary-order-$runId"
      val plan = active(delayed) {
        val captured = new DelayedPlan(log(delayed, path).update().version, usedSeq = 10L, higherSeq = 12L)
        record("C2-pre-advance-plan", delayed, path, app, "planned", Some(captured))
        captured
      }
      active(advanced) {
        assert(log(advanced, path).update().version == plan.tableVersion)
        setTxn(advanced, app, plan.usedSeq, "canary-c2-advanced")
        observe("C2-advanced", advanced, path, app) { merge(advanced, path, 1L) }
      }
      val afterAdvance = log(spark, path).update().version
      assert(afterAdvance == plan.tableVersion + 1L)
      assertCommit(spark, path, afterAdvance, app, plan.usedSeq, "canary-c2-advanced")
      active(delayed) {
        setTxn(delayed, app, plan.usedSeq, "canary-c2-used")
        observe("C2-used-seq", delayed, path, app) { merge(delayed, path, 100L) }
        assert(log(delayed, path).update().version == afterAdvance)
        assertRows(delayed, path, "SELECT 1 AS id, CAST(11 AS BIGINT) AS total UNION ALL SELECT 2, CAST(20 AS BIGINT)")
        setTxn(delayed, app, plan.higherSeq, "canary-c2-planned-gap")
        observe("C2-higher-planned-seq", delayed, path, app) { merge(delayed, path, 1000L) }
        assert(log(delayed, path).update().version == afterAdvance + 1L)
        assert(log(delayed, path).update().transactions.get(app).contains(plan.higherSeq))
        assertCommit(delayed, path, afterAdvance + 1L, app, plan.higherSeq, "canary-c2-planned-gap")
        assertRows(delayed, path, "SELECT 1 AS id, CAST(1011 AS BIGINT) AS total UNION ALL SELECT 2, CAST(20 AS BIGINT)")
      }
    }
  }
}
