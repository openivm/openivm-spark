package org.apache.spark {
  object OpenIvmCostCountBus {
    def drain(sc: SparkContext): Unit = sc.listenerBus.waitUntilEmpty(10000)
  }
}

package org.openivm.spark.bench {

  import org.apache.logging.log4j.LogManager
  import org.apache.logging.log4j.core.appender.AbstractAppender
  import org.apache.logging.log4j.core.config.Property
  import org.apache.logging.log4j.core.layout.PatternLayout
  import org.apache.logging.log4j.core.{LogEvent, Logger}
  import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
  import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
  import org.openivm.spark.parity.base.{InterceptMode, IvmParityMode, IvmParitySpecBase}

  import java.io.File
  import java.nio.charset.StandardCharsets
  import java.nio.file.Files
  import java.util.UUID
  import java.util.concurrent.atomic.AtomicInteger
  import scala.collection.mutable.ArrayBuffer

  /** Machine-independent refresh cost counts: Spark SQL executions, Spark jobs,
  * Delta commits (by table class and operation) and files rewritten in the MV
  * table, per REFRESH. Every refresh is checked with EXCEPT ALL both ways.
  *
  * Run: ivmIt/testOnly org.openivm.spark.bench.RefreshCostCountHarness -- -n org.openivm.spark.tags.MicroBenchmark
  */
  class RefreshCostCountHarness extends RefreshCostCountScenarios with InterceptMode

  abstract class RefreshCostCountScenarios extends IvmParitySpecBase("refresh-cost-count") {
    self: IvmParityMode =>

    // Keep consumed view-delta tables on disk so their commits can be counted.
    override protected def extraSparkConf: Map[String, String] = Map("spark.openivm.gc.enabled" -> "false")

    private final class Buffer(name: String)
        extends AbstractAppender(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
      val lines                                  = ArrayBuffer.empty[String]
      override def append(event: LogEvent): Unit = lines.synchronized(lines += event.getMessage.getFormattedMessage)
    }

    private val sqlExecs = new AtomicInteger(0)
    private val jobs     = new AtomicInteger(0)
    private val sqlDescs = ArrayBuffer.empty[String]

    private lazy val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = jobs.incrementAndGet()
      override def onOtherEvent(e: SparkListenerEvent): Unit = e match {
        case s: SparkListenerSQLExecutionStart =>
          sqlExecs.incrementAndGet()
          sqlDescs.synchronized(sqlDescs += s.description.replace('\n', ' ').take(160))
        case _ => ()
      }
    }

    private def commitFiles(): Set[String] = {
      val out = ArrayBuffer.empty[String]
      def walk(f: File): Unit =
        if (f.isDirectory) {
          if (f.getName == "_delta_log")
            Option(f.listFiles()).getOrElse(Array.empty[File]).foreach { c =>
              if (c.getName.matches("\\d{20}\\.json")) out += c.getAbsolutePath
            }
          else Option(f.listFiles()).getOrElse(Array.empty[File]).foreach(walk)
        }
      walk(new File(warehouseDir))
      out.toSet
    }

    private def classify(path: String): String =
      if (path.contains("/_ivm/views/")) "mv"
      else if (path.contains("/_ivm/view_deltas/")) "view_delta"
      else if (path.contains("/_ivm/staging/")) "staging"
      else if (path.contains("/_openivm/")) "openivm_internal"
      else "other"

    private def commitInfo(path: String): (String, Map[String, String]) = {
      val text = new String(Files.readAllBytes(new File(path).toPath), StandardCharsets.UTF_8)
      val line = text.split("\n").find(_.contains("\"commitInfo\"")).getOrElse("")
      val op   = "\"operation\":\"([^\"]+)\"".r.findFirstMatchIn(line).map(_.group(1)).getOrElse("?")
      val metrics =
        "\"(num[A-Za-z]+)\":\"(\\d+)\"".r.findAllMatchIn(line).map(m => m.group(1) -> m.group(2)).toMap
      op -> metrics
    }

    private val results = ArrayBuffer.empty[String]

    private def measuredRefresh(view: String, round: String, expectedSql: String): Unit = {
      val appender = new Buffer(s"cost-${UUID.randomUUID()}")
      val root     = LogManager.getRootLogger.asInstanceOf[Logger]
      appender.start(); root.addAppender(appender)
      val before = commitFiles()
      org.apache.spark.OpenIvmCostCountBus.drain(spark.sparkContext)
      sqlExecs.set(0); jobs.set(0); sqlDescs.synchronized(sqlDescs.clear())
      try refreshMv(view)
      finally {
        org.apache.spark.OpenIvmCostCountBus.drain(spark.sparkContext)
        root.removeAppender(appender); appender.stop()
      }
      val execs   = sqlExecs.get(); val jobCount = jobs.get()
      val descs   = sqlDescs.synchronized(sqlDescs.toVector)
      val newOnes = (commitFiles() -- before).toSeq.sorted
      val byClass = newOnes.groupBy(classify).map { case (k, v) => k -> v.size }
      val ops     = newOnes.map(p => s"${classify(p)}:${commitInfo(p)._1}")
      def actions(paths: Seq[String], kind: String): Int =
        paths.map { p =>
          new String(Files.readAllBytes(new File(p).toPath), StandardCharsets.UTF_8)
            .split("\n")
            .count(_.startsWith("{\"" + kind + "\""))
        }.sum
      val mvCommits      = newOnes.filter(classify(_) == "mv")
      val mvFilesAdded   = actions(mvCommits, "add")
      val mvFilesRemoved = actions(mvCommits, "remove")
      val filesWritten   = actions(newOnes, "add")
      val lines          = appender.lines.synchronized(appender.lines.toVector)
      val stmtKinds = lines
        .filter(l => l.startsWith("[openivm-perf] ") && l.contains("phase='stmt'") && l.contains(s"view='`$view`'"))
        .flatMap(l => "stmt_kind='([^']+)'".r.findFirstMatchIn(l).map(_.group(1)))
      val outcome = lines
        .find(l => l.contains("phase='end'") && l.contains(s"view='`$view`'"))
        .flatMap(l => "outcome='([^']+)'".r.findFirstMatchIn(l).map(_.group(1)))
        .getOrElse("?")
      lines
        .filter(_.contains("[openivm-mv] refresh view="))
        .foreach(l => println(s"COSTLOG\t$view\t$round\t${l.take(600)}"))
      assertMvCorrect(view, expectedSql)
      val row = Seq(
        view,
        round,
        outcome,
        stmtKinds.size.toString,
        stmtKinds.mkString("|"),
        execs.toString,
        jobCount.toString,
        newOnes.size.toString,
        byClass.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(","),
        ops.mkString("|"),
        mvFilesAdded.toString,
        mvFilesRemoved.toString,
        filesWritten.toString
      ).mkString("\t")
      results += row
      println(s"COSTROW\t$row")
      descs.foreach(d => println(s"COSTSQL\t$view\t$round\t$d"))
      newOnes.foreach { p =>
        val text = new String(Files.readAllBytes(new File(p).toPath), StandardCharsets.UTF_8)
        val line = text.split("\n").find(_.contains("\"commitInfo\"")).getOrElse("")
        println(s"COSTCOMMIT\t$view\t$round\t${classify(p)}\t${line.take(900)}")
      }
    }

    describe("refresh cost counts") {
      it("prints per-refresh statement, job and Delta commit counts", MicroBenchmarkTag) {
        spark.sparkContext.addSparkListener(listener)
        sql("CREATE TABLE cc_fact (id INT, grp STRING, val INT, dim_id INT) USING DELTA")
        sql("CREATE TABLE cc_dim (dim_id INT, region STRING) USING DELTA")
        sql("INSERT INTO cc_dim SELECT CAST(id AS INT), CONCAT('r', CAST(id % 4 AS STRING)) FROM range(20) AS t(id)")
        sql(
          "INSERT INTO cc_fact SELECT CAST(id AS INT), CONCAT('g', CAST(id % 10 AS STRING)), " +
            "CAST(id % 97 AS INT), CAST(id % 20 AS INT) FROM range(2000) AS t(id)"
        )
        val only = sys.props.get("openivm.costcount.views").map(_.split(",").toSet)
        val views = Seq(
          "cc_grp"    -> "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cc_fact GROUP BY grp",
          "cc_grpsum" -> "SELECT grp, SUM(val) AS total FROM cc_fact GROUP BY grp",
          "cc_join" -> ("SELECT d.region, SUM(f.val) AS total, COUNT(*) AS cnt FROM cc_fact f " +
            "JOIN cc_dim d ON f.dim_id = d.dim_id GROUP BY d.region"),
          "cc_proj"   -> "SELECT id, grp, val FROM cc_fact WHERE val > 10",
          "cc_minmax" -> "SELECT grp, MIN(val) AS mn, MAX(val) AS mx FROM cc_fact GROUP BY grp",
          "cc_win"    -> "SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM cc_fact",
          "cc_hav" -> "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cc_fact GROUP BY grp HAVING SUM(val) > 4000",
          "cc_dist" -> "SELECT grp, COUNT(DISTINCT val) AS dv FROM cc_fact GROUP BY grp",
          "cc_left" -> ("SELECT d.region, SUM(f.val) AS total, COUNT(*) AS cnt FROM cc_dim d " +
            "LEFT JOIN cc_fact f ON f.dim_id = d.dim_id GROUP BY d.region"),
          "cc_up"   -> "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM cc_fact GROUP BY grp",
          "cc_down" -> "SELECT cnt, COUNT(*) AS n, SUM(total) AS t FROM cc_up GROUP BY cnt"
        ).filter { case (v, _) => only.forall(_.contains(v)) }
        views.foreach { case (v, q) => sql(s"CREATE MATERIALIZED VIEW $v AS $q") }
        views.foreach { case (v, q) => assertMvCorrect(v, q) }

        def refreshAll(round: String): Unit = views.foreach { case (v, q) => measuredRefresh(v, round, q) }

        // Round 1: batched conflicting DML, including an emptied group.
        sql("INSERT INTO cc_fact VALUES (5000, 'g1', 50, 3), (5001, 'gnew', 7, 4), (5002, 'g2', 1, 5)")
        sql("UPDATE cc_fact SET val = val + 1000 WHERE id IN (5000, 1, 2, 3)")
        sql("UPDATE cc_fact SET grp = 'g3' WHERE id IN (4, 14)")
        sql("DELETE FROM cc_fact WHERE id IN (5002, 7, 8)")
        sql("DELETE FROM cc_fact WHERE grp = 'g9'")
        sql("INSERT INTO cc_fact VALUES (5003, 'g9', 11, 6)")
        sql("DELETE FROM cc_fact WHERE id = 5003")
        sql("UPDATE cc_dim SET region = 'r9' WHERE dim_id = 2")
        refreshAll("mixed")

        // Round 2: insert-only batch.
        sql("INSERT INTO cc_fact VALUES (6000, 'g0', 5, 1), (6001, 'g5', 300, 2), (6002, 'gnew2', 3, 3)")
        refreshAll("insert")

        // Round 3: delete-heavy batch that empties groups.
        sql("DELETE FROM cc_fact WHERE grp IN ('g8', 'gnew', 'gnew2')")
        sql("UPDATE cc_fact SET val = 0 WHERE grp = 'g7'")
        refreshAll("delete")

        // Round 4: no pending changes.
        refreshAll("empty")

        println(
          "COSTHDR\tview\tround\toutcome\tstmts\tstmt_kinds\tsql_execs\tjobs\tcommits\tcommits_by_class\tcommit_ops\tmv_files_added\tmv_files_removed\tfiles_written"
        )
        results.foreach(r => println(s"COSTFINAL\t$r"))
      }
    }
  }
}
