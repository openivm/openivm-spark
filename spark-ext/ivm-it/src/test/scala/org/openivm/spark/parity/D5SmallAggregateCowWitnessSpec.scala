package org.openivm.spark.parity

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.{FeatureGate, RefreshProfileCatalog, RefreshSqlLogCatalog}
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

object D5CowFault {
  val key = "spark.databricks.delta.merge.deletionVectors.persistent"
  val armed = new AtomicBoolean(false)
  val observed = new AtomicReference[SparkSession]()
  val message = "D5 witness rejecting direct MERGE while copy-on-write is active"
}

class D5CowCheckExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectCheckRule { session => plan =>
      if (D5CowFault.armed.get() && session.conf.getOption(D5CowFault.key).contains("false") &&
          plan.exists(_.nodeName.toLowerCase(java.util.Locale.ROOT).contains("merge"))) {
        D5CowFault.observed.set(session)
        throw new IllegalStateException(D5CowFault.message)
      }
    }
}

class D5SmallAggregateCowWitnessSpec extends IvmParitySpecBase("d5-h7b-cow") with CdfMode {
  private val limit = 16L * 1024 * 1024
  override protected def sparkMaster: String = "local[2]"
  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.sql.extensions" -> ("io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions," +
      classOf[D5CowCheckExtension].getName),
    FeatureGate.ProfileRefreshKey -> "true",
    "spark.openivm.queryLog.enabled" -> "true",
    "spark.sql.parquet.compression.codec" -> "uncompressed",
    D5CowFault.key -> "true"
  )

  private def note(s: String): Unit = {
    Files.write(Paths.get(sys.env("D5_COW_WITNESS_RECEIPTS")), (s + "\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    ()
  }

  override def beforeAll(): Unit = {
    val actual = Paths.get(classOf[org.openivm.spark.commands.RefreshMaterializedViewCommand]
      .getProtectionDomain.getCodeSource.getLocation.toURI).toRealPath()
    sys.env.get("D5_COW_EVIDENCE_JAR").foreach(j => require(actual == Paths.get(j).toRealPath(), s"loaded $actual"))
    note(s"command_class_source=$actual")
    super.beforeAll()
  }

  private def layout(mv: String): (Long, Long) = {
    val snapshot = DeltaLog.forTable(spark, new Path(mvDataLocation(mv))).update()
    (snapshot.sizeInBytes, snapshot.allFiles.filter("deletionVector IS NOT NULL").count())
  }

  private def refresh(mv: String, direct: Boolean = false): Unit = {
    RefreshProfileCatalog.removeAll(spark)
    RefreshSqlLogCatalog.removeAll(spark)
    refreshMv(mv)
    val detail = RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n")
    detail should include("outcome=incremental_executed")
    detail should not include "outcome=full_refresh_executed"
    if (direct) {
      val statements = sql("SHOW OPENIVM QUERY LOG").collect().filter(r =>
        r.getString(1).split("\\.").last == mv && r.getString(6) == "rewritten_stmt")
      statements should have length 1
      statements.head.getString(9) should include("__openivm_direct_delta")
    }
    spark.conf.getOption(D5CowFault.key) shouldBe Some("true")
  }

  it("preserves live deletion vectors, exact CDF and cascade results on a small aggregate target") {
    val src = "d5cow_small_src"
    val mv = "d5cow_small_mv"
    val child = "d5cow_small_child"
    val body = s"SELECT grp, SUM(v) AS total FROM $src GROUP BY grp"
    sql(s"CREATE TABLE $src (grp STRING, v INT) USING DELTA")
    sql(s"INSERT INTO $src VALUES ('A',10), ('B',20), ('C',30)")
    sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
    val path = mvDataLocation(mv)
    sql(s"UPDATE $src SET v=9 WHERE grp='A'")
    refresh(mv)
    val (bytes, liveDvs) = layout(mv)
    note(s"small_before bytes=$bytes active_dv_files=$liveDvs")
    bytes should be > 0L
    bytes should be <= limit
    liveDvs should be > 0L
    assertMvCorrect(mv, body)
    sql(s"CREATE MATERIALIZED VIEW $child AS SELECT grp, total FROM $mv")
    sql(s"INSERT INTO $src VALUES ('B',2), ('D',40)")
    val before = mvDataVersion(mv)
    refresh(mv, direct = true)
    val version = mvDataVersion(mv)
    version shouldBe before + 1
    layout(mv)._2 shouldBe 0L
    assertMvCorrect(mv, body)
    val feed = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", version)
      .option("endingVersion", version).load(path).select("grp", "total", "_change_type")
    val expected = sql("SELECT * FROM VALUES ('B',20L,'update_preimage'),('B',22L,'update_postimage')," +
      "('D',40L,'insert') AS changes(grp,total,_change_type)")
    feed.exceptAll(expected).count() shouldBe 0L
    expected.exceptAll(feed).count() shouldBe 0L
    refresh(child)
    assertMvCorrect(child, body)

    sql(s"INSERT INTO $src VALUES ('E',5)")
    sql(s"UPDATE $src SET v=11 WHERE grp='A'")
    sql(s"DELETE FROM $src WHERE grp='C'")
    refresh(mv)
    refresh(child)
    assertMvCorrect(mv, body)
    assertMvCorrect(child, body)
    val noOpBefore = mvDataVersion(mv)
    RefreshProfileCatalog.removeAll(spark)
    refreshMv(mv)
    mvDataVersion(mv) shouldBe noOpBefore
    RefreshProfileCatalog.scanAll(spark).map(_.detail).mkString("\n") should not include "outcome=full_refresh_executed"
    assertMvCorrect(mv, body)
    note(s"small_insert ${before}->$version exact_cdf=true cascade=true mixed_incremental=true noop_version=$noOpBefore")
  }

  it("keeps persistent deletion vectors on a target above 16 MiB") {
    val src = "d5cow_big_src"
    val mv = "d5cow_big_mv"
    val body = s"SELECT grp, SUM(v) AS total FROM $src GROUP BY grp"
    sql(s"CREATE TABLE $src (grp STRING, v INT) USING DELTA")
    sql(s"INSERT INTO $src SELECT sha2(cast(id AS STRING),512), 1 FROM range(350000)")
    sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
    val (bytes, pre) = layout(mv)
    note(s"large_before bytes=$bytes active_dv_files=$pre")
    bytes should be > limit
    pre shouldBe 0L
    sql(s"INSERT INTO $src SELECT sha2('0',512),9")
    refresh(mv, direct = true)
    val (_, post) = layout(mv)
    post should be > 0L
    assertMvCorrect(mv, body)
    note(s"large_after active_dv_files=$post incremental=true")
  }

  it("restores the actual cloned session and propagates a nested direct MERGE failure without committing") {
    val src = "d5cow_fail_src"
    val mv = "d5cow_fail_mv"
    val body = s"SELECT grp, SUM(v) AS total FROM $src GROUP BY grp"
    sql(s"CREATE TABLE $src (grp INT, v INT) USING DELTA")
    sql(s"INSERT INTO $src VALUES (1,10),(2,20)")
    sql(s"CREATE MATERIALIZED VIEW $mv AS $body")
    sql(s"INSERT INTO $src VALUES (1,2)")
    val version = mvDataVersion(mv)
    spark.conf.getOption(D5CowFault.key) shouldBe Some("true")
    D5CowFault.observed.set(null)
    D5CowFault.armed.set(true)
    val error = try intercept[Exception](refreshMv(mv))
    finally D5CowFault.armed.set(false)
    Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).map(_.getMessage)
      .exists(s => Option(s).exists(_.contains(D5CowFault.message))) shouldBe true
    val clone = Option(D5CowFault.observed.getAndSet(null)).getOrElse(fail("nested MERGE check not observed"))
    (clone eq spark) shouldBe false
    clone.conf.getOption(D5CowFault.key) shouldBe Some("true")
    spark.conf.getOption(D5CowFault.key) shouldBe Some("true")
    mvDataVersion(mv) shouldBe version
    refresh(mv, direct = true)
    assertMvCorrect(mv, body)
    note(s"failure unchanged_version=$version clone_during=false clone_after=true caller_after=true retry_incremental=true")
  }
}
