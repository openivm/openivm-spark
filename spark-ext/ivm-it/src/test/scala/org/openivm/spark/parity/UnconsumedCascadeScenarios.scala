package org.openivm.spark.parity

import org.openivm.spark.parity.base.IvmParitySpecBase

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, Logger}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/** Grouped aggregate refreshes skip work whose only reader is a downstream MV, and the
  * Spark-side empty-group DELETE when OpenIVM's program already removed those groups.
  * Every refresh follows a batch of conflicting DML and is checked with EXCEPT ALL both ways.
  */
abstract class UnconsumedCascadeScenarios extends IvmParitySpecBase("unconsumed-cascade") {
  self: org.openivm.spark.parity.base.IvmParityMode =>

  private final class StmtAppender(name: String)
      extends AbstractAppender(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
    private val buffer = ArrayBuffer.empty[String]
    override def append(event: LogEvent): Unit =
      buffer.synchronized(buffer += event.getMessage.getFormattedMessage)
    def messages: Seq[String] = buffer.synchronized(buffer.toVector)
  }

  /** Refresh `view` and return the `stmt_kind` of every statement it executed. */
  private def refreshStatementKinds(view: String): Seq[String] = {
    val appender = new StmtAppender(s"uc-${UUID.randomUUID()}")
    val root     = LogManager.getRootLogger.asInstanceOf[Logger]
    appender.start()
    root.addAppender(appender)
    try refreshMv(view)
    finally {
      root.removeAppender(appender)
      appender.stop()
    }
    appender.messages
      .filter(m => m.startsWith("[openivm-perf] ") && m.contains("phase='stmt'") && m.contains(s"view='`$view`'"))
      .flatMap(m => "stmt_kind='([^']+)'".r.findFirstMatchIn(m).map(_.group(1)))
  }

  /** Inserts, updates and deletes that overlap on the same rows and groups, then empties `emptied`. */
  private def conflictingBatch(table: String, round: Int, emptied: String): Unit = {
    val base = 1000 * round
    sql(s"INSERT INTO $table VALUES ($base, 'a', 5), (${base + 1}, 'b', 7), (${base + 2}, 'n$round', 9)")
    sql(s"UPDATE $table SET val = val + 100 WHERE id IN ($base, 1, 2)")
    sql(s"UPDATE $table SET grp = 'c' WHERE id IN (${base + 1}, 3)")
    sql(s"DELETE FROM $table WHERE id IN (${base + 2}, 4)")
    sql(s"DELETE FROM $table WHERE grp = '$emptied'")
    sql(s"INSERT INTO $table VALUES (${base + 3}, '$emptied', 11)")
    sql(s"DELETE FROM $table WHERE id = ${base + 3}")
  }

  private def seed(table: String): Unit = {
    sql(s"CREATE TABLE $table (id INT, grp STRING, val INT) USING DELTA")
    sql(
      s"INSERT INTO $table SELECT CAST(id AS INT), CHAR(97 + CAST(id % 6 AS INT)), " +
        s"CAST(id % 13 AS INT) FROM range(60) AS t(id)"
    )
  }

  describe("grouped aggregates without a downstream MV") {
    it("skip cascade rows and the duplicate empty-group delete") {
      seed("uc_fact")
      val views = Seq(
        "uc_grp"    -> "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM uc_fact GROUP BY grp",
        "uc_sum"    -> "SELECT grp, SUM(val) AS total FROM uc_fact GROUP BY grp",
        "uc_avg"    -> "SELECT grp, AVG(CAST(val AS DOUBLE)) AS avg_val, COUNT(val) AS n FROM uc_fact GROUP BY grp",
        "uc_hav"    -> "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM uc_fact GROUP BY grp HAVING SUM(val) > 60",
        "uc_minmax" -> "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM uc_fact GROUP BY grp"
      )
      views.foreach { case (view, query) => sql(s"CREATE MATERIALIZED VIEW $view AS $query") }

      Seq("f", "e", "d").zipWithIndex.foreach { case (emptied, i) =>
        conflictingBatch("uc_fact", i + 1, emptied)
        views.foreach { case (view, query) =>
          val kinds = refreshStatementKinds(view)
          assertMvCorrect(view, query)
          if (view == "uc_grp" || view == "uc_sum") {
            withClue(s"$view statements: ${kinds.mkString(", ")}") {
              kinds should contain("merge")
              kinds should not contain "count_monoid_cleanup"
              kinds should not contain "insert_into"
            }
          }
        }
      }
    }
  }

  describe("a grouped aggregate with a downstream MV") {
    it("keeps publishing its cascade rows") {
      seed("uc_chain_fact")
      val upstream   = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM uc_chain_fact GROUP BY grp"
      val downstream = "SELECT cnt, COUNT(*) AS groups, SUM(total) AS total FROM uc_chain_up GROUP BY cnt"
      sql(s"CREATE MATERIALIZED VIEW uc_chain_up AS $upstream")
      sql(s"CREATE MATERIALIZED VIEW uc_chain_down AS $downstream")

      Seq("f", "e").zipWithIndex.foreach { case (emptied, i) =>
        conflictingBatch("uc_chain_fact", i + 1, emptied)
        val kinds = refreshStatementKinds("uc_chain_up")
        assertMvCorrect("uc_chain_up", upstream)
        refreshMv("uc_chain_down")
        assertMvCorrect("uc_chain_down", downstream)
        withClue(s"uc_chain_up statements: ${kinds.mkString(", ")}") {
          kinds should contain("insert_into")
        }
      }
    }
  }
}
