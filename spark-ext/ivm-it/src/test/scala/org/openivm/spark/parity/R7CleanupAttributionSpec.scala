package org.openivm.spark.parity

import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, LocalFileSystem, Path}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.openivm.spark.common.{StagingDelta, StagingDeltaView}
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.sql.Timestamp

object R7CleanupFs {
  private val current                = new ThreadLocal[java.lang.Boolean]()
  def active: Boolean                = java.lang.Boolean.TRUE == current.get()
  def active_=(value: Boolean): Unit = if (value) current.set(true) else current.remove()
  var deltaLogCalls: Int             = 0
  var uncacheStackCalls: Int         = 0
  def observe(path: Path): Unit = if (active && path.toString.contains("_delta_log")) {
    deltaLogCalls += 1
    if (R7CleanupProbe.inUncache) uncacheStackCalls += 1
  }
}

class R7CleanupFs extends LocalFileSystem {
  override def listStatus(path: Path): Array[FileStatus] = {
    R7CleanupFs.observe(path)
    super.listStatus(path)
  }
  override def getFileStatus(path: Path): FileStatus = {
    R7CleanupFs.observe(path)
    super.getFileStatus(path)
  }
  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    R7CleanupFs.observe(path)
    super.open(path, bufferSize)
  }
}

class R7CleanupAttributionSpec extends IvmParitySpecBase("r7-cleanup-attribution") with InterceptMode {
  override protected def extraSparkConf: Map[String, String] = Map(
    "spark.sql.extensions" ->
      ("io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions," +
        "org.openivm.spark.parity.R7CleanupProbeExtensions"),
    "spark.hadoop.fs.file.impl"               -> classOf[R7CleanupFs].getName,
    "spark.hadoop.fs.file.impl.disable.cache" -> "true"
  )

  private val output = Paths.get(sys.env.getOrElse("R7_CLEANUP_OUT", "target/r7-cleanup-attribution.tsv"))

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

  private def timed(caseName: String, pair: Int, arm: String, operation: String)(body: => Unit): Unit = {
    R7CleanupProbe.trackers.clear()
    R7CleanupProbe.track = true
    R7CleanupFs.deltaLogCalls = 0
    R7CleanupFs.uncacheStackCalls = 0
    R7CleanupFs.active = true
    val start     = System.nanoTime()
    var succeeded = false
    try {
      body
      succeeded = true
    } finally {
      val elapsed = System.nanoTime() - start
      R7CleanupFs.active = false
      R7CleanupProbe.track = false
      val (phases, unionMs) = R7CleanupProbe.analysisCoverage
      val analysis          = unionMs.fold("unavailable")(_.toString)
      emit(
        s"api-probe-only\t$caseName\t$pair\t$arm\t$operation\t$elapsed\t$phases\t$analysis\t" +
          s"${R7CleanupFs.deltaLogCalls}\t${R7CleanupFs.uncacheStackCalls}\t$succeeded"
      )
    }
  }

  private def runArm(
      createSql: String,
      alias: String,
      caseName: String,
      pair: Int,
      arm: String,
      cached: Boolean
  ): Unit = {
    val dependent = s"${alias}_dependent"
    spark.sql(createSql).collect()
    spark.sql(s"CREATE TEMP VIEW $dependent AS SELECT id FROM $alias WHERE id % 2 = 0").collect()
    if (cached) {
      spark.catalog.cacheTable(alias, StorageLevel.DISK_ONLY)
      spark.catalog.cacheTable(dependent, StorageLevel.DISK_ONLY)
      spark.table(alias).count()
      spark.table(dependent).count()
    }
    val aliasData     = spark.table(alias)
    val dependentData = spark.table(dependent)
    (aliasData.storageLevel != StorageLevel.NONE) shouldBe cached
    (dependentData.storageLevel != StorageLevel.NONE) shouldBe cached
    try {
      if (arm == "control")
        timed(caseName, pair, arm, "explicit_uncache") { spark.catalog.uncacheTable(alias) }
      timed(caseName, pair, arm, "sql_drop") { spark.sql(s"DROP VIEW IF EXISTS $alias"); () }
      spark.sessionState.catalog.getTempView(alias).isEmpty shouldBe true
      aliasData.storageLevel shouldBe StorageLevel.NONE
      dependentData.storageLevel shouldBe StorageLevel.NONE
      spark.sessionState.catalog.getTempView(dependent).isDefined shouldBe true
    } finally {
      spark.sql(s"DROP VIEW IF EXISTS $dependent").collect()
      spark.sql(s"DROP VIEW IF EXISTS $alias").collect()
    }
  }

  describe("intercept SQL temp-view API attribution, not primary-workload performance") {
    it("splits explicit uncache, planner analysis and DROP with uncached and cached dependents") {
      emit(s"runtime\t${spark.version}\t${System.getProperty("java.version")}\tlocal-io\tinjected_delay_ms=0")
      emit(
        "evidence\tcase\tpair\tarm\toperation\twall_ns\tanalysis_phase_count\tanalysis_union_ms\t" +
          "delta_log_api_calls\tuncache_stack_delta_log_api_calls\tsucceeded"
      )
      val stagingPath = new File(warehouseDir, "r7cl_attr_staging").getAbsolutePath
      spark.range(100).write.format("delta").save(stagingPath)
      val source = "default.r7cl_attr"
      val schema = StructType(Seq(StructField("id", LongType)))
      val createSql = StagingDeltaView.buildSourceDeltaViewSql(
        source,
        schema,
        Seq(StagingDelta(source, StagingDelta.OpTypes.Insert, stagingPath, new Timestamp(0L), Seq.empty))
      )
      val alias = StagingDeltaView.deltaViewName(source)
      Seq(false, true).foreach { cached =>
        Seq("control", "candidate").foreach(arm => runArm(createSql, alias, s"warmup,cached=$cached", 0, arm, cached))
        (1 to 3).foreach { pair =>
          val order = if (pair % 2 == 1) Seq("control", "candidate") else Seq("candidate", "control")
          order.foreach(arm => runArm(createSql, alias, s"cached=$cached", pair, arm, cached))
        }
      }
    }
  }
}
