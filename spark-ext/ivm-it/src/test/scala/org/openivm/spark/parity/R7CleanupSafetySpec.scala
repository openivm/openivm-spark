package org.openivm.spark.parity

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.storage.StorageLevel
import org.openivm.spark.commands.CommandConcurrencyInjection
import org.openivm.spark.common.RefreshSqlLogCatalog
import org.openivm.spark.parity.base.{CdfMode, InterceptMode, IvmParityMode, IvmParitySpecBase}
import org.openivm.spark.parser.IvmParser

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.mutable

object R7CleanupProbe {
  var armed: Boolean                                        = false
  var failDrop: Set[String]                                 = Set.empty
  var failUncache: Boolean                                  = false
  var failAnalysis: Boolean                                 = false
  var uncacheFailures: Int                                  = 0
  var analysisFailures: Int                                 = 0
  val drops: mutable.ArrayBuffer[String]                    = mutable.ArrayBuffer.empty
  val trackers: mutable.LinkedHashSet[QueryPlanningTracker] = mutable.LinkedHashSet.empty
  var track: Boolean                                        = false
  private val dropPattern = """(?is)^\s*DROP\s+VIEW\s+IF\s+EXISTS\s+`?(openivm_delta_\w+)`?\s*;?\s*$""".r

  def inUncache: Boolean =
    Thread.currentThread().getStackTrace.exists(_.getMethodName == "uncacheTable")

  def checkIdentifier(name: String): Unit =
    if (armed && failUncache && name.startsWith("openivm_delta_") && inUncache) {
      uncacheFailures += 1
      throw new IllegalStateException("r7 injected explicit uncache failure")
    }

  def checkDrop(sql: String): Unit = if (armed) {
    sql match {
      case dropPattern(name) =>
        drops += name
        if (failDrop(name)) throw new IllegalStateException(s"r7 injected DROP failure: $name")
      case _ => ()
    }
  }

  def reset(): Unit = {
    armed = false
    failDrop = Set.empty
    failUncache = false
    failAnalysis = false
    uncacheFailures = 0
    analysisFailures = 0
    drops.clear()
    trackers.clear()
    track = false
  }
}

class R7CleanupProbeParser(session: SparkSession, parser: ParserInterface) extends IvmParser(session, parser) {
  override def parsePlan(sql: String): LogicalPlan = {
    R7CleanupProbe.checkDrop(sql)
    super.parsePlan(sql)
  }
  override def parseTableIdentifier(sql: String): TableIdentifier = {
    R7CleanupProbe.checkIdentifier(sql)
    super.parseTableIdentifier(sql)
  }
  override def parseMultipartIdentifier(sql: String): Seq[String] = {
    R7CleanupProbe.checkIdentifier(sql)
    super.parseMultipartIdentifier(sql)
  }
}

class R7CleanupProbeExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser((session, parser) => new R7CleanupProbeParser(session, parser))
    extensions.injectResolutionRule(_ =>
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          if (R7CleanupProbe.track)
            QueryPlanningTracker.get.foreach(R7CleanupProbe.trackers.add)
          if (R7CleanupProbe.armed && R7CleanupProbe.failAnalysis && R7CleanupProbe.inUncache) {
            R7CleanupProbe.analysisFailures += 1
            throw new IllegalStateException("r7 injected uncache analysis failure")
          }
          plan
        }
      }
    )
  }
}

abstract class R7CleanupSafetyScenarios extends IvmParitySpecBase("r7-cleanup-safety") {
  self: IvmParityMode =>

  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.openivm.queryLog.enabled" -> "true",
    "spark.sql.extensions" ->
      "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions," +
      "org.openivm.spark.parity.R7CleanupProbeExtensions"
  )

  private val output    = Paths.get(sys.env.getOrElse("R7_CLEANUP_OUT", "target/r7-cleanup-safety.tsv"))
  private val candidate = sys.env.getOrElse("R7_CLEANUP_ARM", "control") == "candidate"
  private case class CachedAlias(name: String, dependent: String, plan: LogicalPlan, dependentPlan: LogicalPlan)

  private def emit(line: String): Unit = {
    Files.createDirectories(output.toAbsolutePath.getParent)
    Files.write(
      output,
      (line + "\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    RefreshSqlLogCatalog.ensureTables(spark)
    emit(s"runtime\t${spark.version}\t${System.getProperty("java.version")}\t$modeLabel\tcandidate=$candidate")
  }

  private def fixture(slug: String): (String, String, String) = {
    val table = s"r7cl_${modeLabel.replace("-", "_")}_$slug"
    val mv    = s"${table}_mv"
    sql(s"CREATE TABLE $table (k INT, v INT) USING DELTA")
    sql(s"INSERT INTO $table VALUES (1, 10)")
    val body = s"SELECT k, SUM(v) AS total FROM $table GROUP BY k"
    sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
    sql(s"INSERT INTO $table VALUES (1, 20), (2, 30)")
    RefreshSqlLogCatalog.removeAll(spark)
    (table, mv, body)
  }

  private def cacheAlias(table: String): CachedAlias = {
    val alias     = s"openivm_delta_$table"
    val dependent = s"${alias}_dependent"
    spark.sessionState.catalog.getTempView(alias).isDefined shouldBe true
    spark.catalog.cacheTable(alias, StorageLevel.DISK_ONLY)
    spark.sql(s"CREATE OR REPLACE TEMP VIEW $dependent AS SELECT k, v FROM $alias WHERE k = 1")
    spark.catalog.cacheTable(dependent, StorageLevel.DISK_ONLY)
    spark.table(alias).count()
    spark.table(dependent).count()
    val plan    = spark.table(alias).queryExecution.normalized
    val depPlan = spark.table(dependent).queryExecution.normalized
    spark.sharedState.cacheManager.lookupCachedData(plan).nonEmpty shouldBe true
    spark.sharedState.cacheManager.lookupCachedData(depPlan).nonEmpty shouldBe true
    CachedAlias(alias, dependent, plan, depPlan)
  }

  private def assertUncached(cache: CachedAlias): Unit = {
    // Check saved plans before dropping the dependent; that extra DROP could hide a leak.
    spark.sharedState.cacheManager.lookupCachedData(cache.plan).isEmpty shouldBe true
    spark.sharedState.cacheManager.lookupCachedData(cache.dependentPlan).isEmpty shouldBe true
    spark.sessionState.catalog.getTempView(cache.dependent).isDefined shouldBe true
  }

  private def assertLogged(mv: String, aliases: Set[String]): Unit = {
    val rows       = RefreshSqlLogCatalog.scanAll(spark).filter(_.viewName.split("\\.").last == mv)
    val registered = rows.filter(_.category == "register_source_delta")
    registered.nonEmpty shouldBe true
    val cleanup = rows.filter(_.category == "drop_cleanup")
    aliases.foreach(alias => cleanup.exists(_.sqlText.contains(alias)) shouldBe true)
    rows.exists(_.category == "full_refresh_stmt") shouldBe false
    cleanup.foreach(r =>
      emit(s"cleanup\t$modeLabel\t$mv\t${r.refreshId}\t${r.stmtOrder}\t${r.durationMs}\t${r.sqlText}")
    )
  }

  private def finish(caches: Seq[CachedAlias]): Unit = {
    R7CleanupProbe.reset()
    caches.foreach { cache =>
      spark.sql(s"DROP VIEW IF EXISTS `${cache.dependent}`")
      spark.sql(s"DROP VIEW IF EXISTS `${cache.name}`")
    }
  }

  describe("actual source-delta cleanup") {
    it("drops the actual registered alias and flushes its query-log row") {
      val (table, mv, body) = fixture("uncached")
      var seen              = false
      try {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          val view = spark.sessionState.catalog.getTempView(s"openivm_delta_$table").get
          seen = true
          emit(s"view\t$modeLabel\t${view.desc.viewText.isDefined}\t${view.child.nodeName}")
          R7CleanupProbe.armed = true
        } {
          refreshMv(mv)
        }
        R7CleanupProbe.armed = false
        seen shouldBe true
        R7CleanupProbe.drops.toSet should contain(s"openivm_delta_$table")
        spark.sessionState.catalog.getTempView(s"openivm_delta_$table").isEmpty shouldBe true
        assertLogged(mv, Set(s"openivm_delta_$table"))
        assertMvCorrect(mv, body)
      } finally R7CleanupProbe.reset()
    }

    it("removes the cached actual alias and its still-live dependent cache") {
      val (table, mv, body) = fixture("cached")
      var caches            = Seq.empty[CachedAlias]
      try {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          caches = Seq(cacheAlias(table))
          R7CleanupProbe.armed = true
        } {
          refreshMv(mv)
        }
        R7CleanupProbe.armed = false
        caches.size shouldBe 1
        caches.foreach(assertUncached)
        spark.sessionState.catalog.getTempView(caches.head.name).isEmpty shouldBe true
        assertLogged(mv, caches.map(_.name).toSet)
        assertMvCorrect(mv, body)
      } finally finish(caches)
    }

    it("retains best-effort uncaching and query-log flush when DROP fails") {
      val (table, mv, body) = fixture("drop_failure")
      var caches            = Seq.empty[CachedAlias]
      try {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          caches = Seq(cacheAlias(table))
          R7CleanupProbe.failDrop = caches.map(_.name).toSet
          R7CleanupProbe.armed = true
        } {
          refreshMv(mv)
        }
        R7CleanupProbe.armed = false
        R7CleanupProbe.drops.toSet shouldBe caches.map(_.name).toSet
        caches.foreach(assertUncached)
        spark.sessionState.catalog.getTempView(caches.head.name).isDefined shouldBe true
        assertLogged(mv, caches.map(_.name).toSet)
        assertMvCorrect(mv, body)
      } finally finish(caches)
    }

    it("still drops and uncaches dependents after explicit uncache failure") {
      val (table, mv, body) = fixture("uncache_failure")
      var caches            = Seq.empty[CachedAlias]
      try {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          caches = Seq(cacheAlias(table))
          R7CleanupProbe.failUncache = true
          R7CleanupProbe.armed = true
        } {
          refreshMv(mv)
        }
        R7CleanupProbe.armed = false
        R7CleanupProbe.uncacheFailures shouldBe (if (candidate) 0 else 1)
        caches.foreach(assertUncached)
        spark.sessionState.catalog.getTempView(caches.head.name).isEmpty shouldBe true
        assertLogged(mv, caches.map(_.name).toSet)
        assertMvCorrect(mv, body)
      } finally finish(caches)
    }

    it("still drops and uncaches dependents when Spark swallows a reanalysis failure") {
      val (table, mv, body) = fixture("analysis_failure")
      var caches            = Seq.empty[CachedAlias]
      try {
        CommandConcurrencyInjection.withBeforeRefreshFinalize {
          caches = Seq(cacheAlias(table))
          R7CleanupProbe.failAnalysis = true
          R7CleanupProbe.armed = true
        } {
          refreshMv(mv)
        }
        R7CleanupProbe.armed = false
        if (candidate) R7CleanupProbe.analysisFailures shouldBe 0
        else R7CleanupProbe.analysisFailures should be > 0
        caches.foreach(assertUncached)
        spark.sessionState.catalog.getTempView(caches.head.name).isEmpty shouldBe true
        assertLogged(mv, caches.map(_.name).toSet)
        assertMvCorrect(mv, body)
      } finally finish(caches)
    }

    it("preserves the original REFRESH exception when DROP also fails") {
      val (table, mv, _) = fixture("primary_failure")
      val primary        = new IllegalStateException("r7 primary refresh failure")
      var caches         = Seq.empty[CachedAlias]
      try {
        val thrown = intercept[Throwable] {
          CommandConcurrencyInjection.withBeforeRefreshFinalize {
            caches = Seq(cacheAlias(table))
            R7CleanupProbe.failDrop = caches.map(_.name).toSet
            R7CleanupProbe.armed = true
            throw primary
          } {
            refreshMv(mv)
          }
        }
        R7CleanupProbe.armed = false
        Iterator.iterate(thrown)(_.getCause).takeWhile(_ != null).exists(_ eq primary) shouldBe true
        caches.size shouldBe 1
        R7CleanupProbe.drops.toSet shouldBe caches.map(_.name).toSet
        caches.foreach(assertUncached)
        assertLogged(mv, caches.map(_.name).toSet)
      } finally finish(caches)
    }
  }
}

class R7CleanupInterceptSpec extends R7CleanupSafetyScenarios with InterceptMode
class R7CleanupCdfSpec       extends R7CleanupSafetyScenarios with CdfMode
