package org.openivm.spark.parity

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.openivm.spark.commands.{CdfCreateSnapshots, CommandConcurrencyInjection}
import org.openivm.spark.common.{ChangeWatermark, MvCatalog, RefreshTypeCode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.openivm.spark.parity.base.{CdfMode, IvmParitySpecBase}

class CdfCreateBaselineSpec extends IvmParitySpecBase("cdf-create-baseline-race") with CdfMode {
  override protected def sparkMaster: String = "local[2]"

  it("observes CREATE data and its captured CDF baseline under an intervening commit") {
    sql("CREATE TABLE cdf_create_race_src (k INT) USING DELTA").collect()
    sql("INSERT INTO cdf_create_race_src VALUES (1)").collect()
    val baselineVersion               = sql("DESCRIBE HISTORY cdf_create_race_src LIMIT 1").head().getLong(0)
    val entered                       = new CountDownLatch(1)
    val release                       = new CountDownLatch(1)
    implicit val ec: ExecutionContext = ExecutionContext.global
    CommandConcurrencyInjection.withBeforeCreateDataWrite {
      entered.countDown()
      require(release.await(180, TimeUnit.SECONDS), "CREATE data-write hook was not released")
    } {
      val create = Future {
        sql(
          "CREATE MATERIALIZED VIEW cdf_create_race_mv AS " +
            "SELECT k, COUNT(*) AS n FROM cdf_create_race_src GROUP BY k"
        ).collect()
      }
      try {
        require(entered.await(180, TimeUnit.SECONDS), "CREATE data-write hook was not reached")
        sql("INSERT INTO cdf_create_race_src VALUES (1)").collect()
      } finally release.countDown()
      Await.result(create, 300.seconds)
    }
    val meta   = MvCatalog.lookup(spark, TableIdentifier("cdf_create_race_mv")).get
    val source = meta.sourceTables.find(_.endsWith("cdf_create_race_src")).get
    assert(meta.refreshType != RefreshTypeCode.FullRefresh)
    assert(meta.changeWatermarks.get(source).contains(ChangeWatermark.DeltaVersion(baselineVersion)))
    val recovered = sql(s"DESCRIBE DETAIL delta.`${meta.location}`").head().getAs[Map[String, String]]("properties")
    assert(recovered(CdfCreateSnapshots.Property) == meta.properties(CdfCreateSnapshots.Property))
    CdfCreateSnapshots.verifyRecovered(spark, recovered, meta.sourceTables)
    assert(sql("DESCRIBE HISTORY cdf_create_race_src LIMIT 1").head().getLong(0) > baselineVersion)
    assertMvCorrect(
      "cdf_create_race_mv",
      s"SELECT k, COUNT(*) AS n FROM cdf_create_race_src VERSION AS OF $baselineVersion GROUP BY k"
    )
    val expectedSql = "SELECT k, COUNT(*) AS n FROM cdf_create_race_src GROUP BY k"
    refreshMv("cdf_create_race_mv")
    assertMvCorrect("cdf_create_race_mv", expectedSql)
    assert(
      MvCatalog.lookup(spark, TableIdentifier("cdf_create_race_mv")).get.refreshType != RefreshTypeCode.FullRefresh
    )
    refreshMv("cdf_create_race_mv")
    assertMvCorrect("cdf_create_race_mv", expectedSql)
    sql("INSERT INTO cdf_create_race_src VALUES (2)").collect()
    refreshMv("cdf_create_race_mv")
    assertMvCorrect("cdf_create_race_mv", expectedSql)
  }

  it("refuses publication when a captured source is rebound after the initial write") {
    val oldPath = s"$warehouseDir/cdf_create_rebind_old"
    val newPath = s"$warehouseDir/cdf_create_rebind_new"
    sql(s"CREATE TABLE cdf_create_rebind_src (k INT) USING DELTA LOCATION '$oldPath'").collect()
    sql("INSERT INTO cdf_create_rebind_src VALUES (1)").collect()
    val entered                       = new CountDownLatch(1)
    val release                       = new CountDownLatch(1)
    implicit val ec: ExecutionContext = ExecutionContext.global
    CommandConcurrencyInjection.withAfterCreateDataWrite {
      entered.countDown()
      require(release.await(180, TimeUnit.SECONDS), "CREATE post-write hook was not released")
    } {
      val create = Future {
        sql(
          "CREATE MATERIALIZED VIEW cdf_create_rebind_mv AS " +
            "SELECT k, COUNT(*) AS n FROM cdf_create_rebind_src GROUP BY k"
        ).collect()
      }
      try {
        require(entered.await(180, TimeUnit.SECONDS), "CREATE post-write hook was not reached")
        sql("DROP TABLE cdf_create_rebind_src").collect()
        sql(s"CREATE TABLE cdf_create_rebind_src (k INT) USING DELTA LOCATION '$newPath'").collect()
      } finally release.countDown()
      val error = intercept[IllegalStateException] { Await.result(create, 300.seconds) }
      assert(error.getMessage.contains("changed physical Delta identity"))
    }
    assert(!spark.catalog.tableExists("cdf_create_rebind_mv"))
    assert(MvCatalog.lookup(spark, TableIdentifier("cdf_create_rebind_mv")).isEmpty)
  }

  it("binds sources beneath an ordinary Spark view") {
    sql("CREATE TABLE cdf_create_view_src (k INT) USING DELTA").collect()
    sql("INSERT INTO cdf_create_view_src VALUES (1), (2)").collect()
    val baselineVersion = sql("DESCRIBE HISTORY cdf_create_view_src LIMIT 1").head().getLong(0)
    sql("CREATE VIEW cdf_create_source_view AS SELECT k FROM cdf_create_view_src WHERE k > 1").collect()
    val plan     = spark.sql("SELECT k FROM cdf_create_source_view").queryExecution.analyzed
    val source   = "default.cdf_create_view_src"
    val captured = CdfCreateSnapshots.capture(spark, Seq(source), Map(source -> spark.table(source).schema), Nil, plan)
    sql("INSERT INTO cdf_create_view_src VALUES (3)").collect()
    val actual   = captured.initialRows(spark, plan)
    val expected = spark.sql(s"SELECT k FROM cdf_create_view_src VERSION AS OF $baselineVersion WHERE k > 1")
    assert(actual.exceptAll(expected).count() == 0L)
    assert(expected.exceptAll(actual).count() == 0L)
  }

  it("binds physical Delta path reads even when they have no catalog source name") {
    sql("CREATE TABLE cdf_create_path_src (k INT) USING DELTA").collect()
    sql("INSERT INTO cdf_create_path_src VALUES (1)").collect()
    val detail   = sql("DESCRIBE DETAIL cdf_create_path_src").head()
    val path     = detail.getAs[String]("location")
    val version  = sql("DESCRIBE HISTORY cdf_create_path_src LIMIT 1").head().getLong(0)
    val plan     = spark.sql(s"SELECT k FROM delta.`$path`").queryExecution.analyzed
    val captured = CdfCreateSnapshots.capture(spark, Nil, Map.empty, Nil, plan)
    assert(captured.versions.isEmpty)
    CdfCreateSnapshots.verifyRecovered(spark, captured.properties, Nil)
    sql("INSERT INTO cdf_create_path_src VALUES (2)").collect()
    val actual   = captured.initialRows(spark, plan)
    val expected = spark.sql(s"SELECT k FROM delta.`$path` VERSION AS OF $version")
    assert(actual.exceptAll(expected).count() == 0L)
    assert(expected.exceptAll(actual).count() == 0L)
  }

  private def createRecoverablePath(source: String, mv: String): (String, Long, String) = {
    sql(s"CREATE TABLE $source (k INT) USING DELTA").collect()
    sql(s"INSERT INTO $source VALUES (1)").collect()
    val query    = s"SELECT k FROM $source"
    val plan     = spark.sql(query).queryExecution.analyzed
    val captured = CdfCreateSnapshots.capture(spark, Seq(source), Map(source -> spark.table(source).schema), Nil, plan)
    val version  = captured.versions(source)
    val properties = captured.properties ++ Map(
      "_ivm_create_watermarks_v1" -> "true",
      s"_ivm_watermark:$source"   -> ChangeWatermark.DeltaVersion(version).encode
    )
    val props = properties.toSeq
      .map { case (key, value) =>
        s"'${key.replace("'", "''")}'='${value.replace("'", "''")}'"
      }
      .mkString(", ")
    val root     = spark.conf.get("spark.sql.warehouse.dir").stripSuffix("/")
    val location = s"$root/_ivm/views/default/$mv"
    sql(s"CREATE TABLE delta.`$location` USING DELTA TBLPROPERTIES ($props) AS $query").collect()
    (query, version, location)
  }

  it("reuses a committed CREATE path with its original source snapshot") {
    val source              = "default.cdf_create_recover_src"
    val (query, version, _) = createRecoverablePath(source, "cdf_create_recover_mv")
    sql(s"INSERT INTO $source VALUES (2)").collect()
    sql(s"CREATE MATERIALIZED VIEW default.cdf_create_recover_mv AS $query").collect()
    assertMvCorrect("default.cdf_create_recover_mv", s"SELECT k FROM $source VERSION AS OF $version")
    refreshMv("default.cdf_create_recover_mv")
    assertMvCorrect("default.cdf_create_recover_mv", query)
    assert(
      MvCatalog.lookup(spark, TableIdentifier("cdf_create_recover_mv", Some("default"))).get.refreshType !=
        RefreshTypeCode.FullRefresh
    )
  }

  it("does not publish recovered CREATE data if its source is rebound during recovery") {
    val source                        = "default.cdf_create_recover_rebind_src"
    val (query, _, location)          = createRecoverablePath(source, "cdf_create_recover_rebind_mv")
    val entered                       = new CountDownLatch(1)
    val release                       = new CountDownLatch(1)
    implicit val ec: ExecutionContext = ExecutionContext.global
    CommandConcurrencyInjection.withAfterCreateDataWrite {
      entered.countDown()
      require(release.await(180, TimeUnit.SECONDS), "CREATE recovery hook was not released")
    } {
      val create = Future {
        sql(s"CREATE MATERIALIZED VIEW default.cdf_create_recover_rebind_mv AS $query").collect()
      }
      try {
        require(entered.await(180, TimeUnit.SECONDS), "CREATE recovery hook was not reached")
        sql(s"DROP TABLE $source").collect()
        sql(s"CREATE TABLE $source (k INT) USING DELTA").collect()
      } finally release.countDown()
      val error = intercept[IllegalArgumentException] { Await.result(create, 300.seconds) }
      assert(error.getMessage.contains("does not match its saved physical snapshot"))
    }
    assert(!spark.catalog.tableExists("default.cdf_create_recover_rebind_mv"))
    assert(io.delta.tables.DeltaTable.isDeltaTable(spark, location))
  }
}
