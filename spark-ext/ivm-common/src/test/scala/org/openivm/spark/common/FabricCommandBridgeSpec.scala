package org.openivm.spark.common

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.openivm.spark.insights.{OpenIvmInsightsBroker, OpenIvmInsightsContract}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.nio.file.Files
import java.util.{Properties, UUID}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.jdk.CollectionConverters._

object FabricCommandProbe {
  @volatile var entered: CountDownLatch = new CountDownLatch(0)
  @volatile var proceed: CountDownLatch = new CountDownLatch(0)
  var thread: Long = 0L
  var values: Seq[String] = Seq.empty
  val keys: Seq[String] = Seq(
    "openivm.request_id", "openivm.run_id", "openivm.node_id", "openivm.campaign_id",
    "openivm.correlation_id", "openivm.phase", "spark.jobGroup.id", "spark.job.description",
    "spark.job.interruptOnCancel"
  )

  def parser(delegate: ParserInterface): ParserInterface =
    Proxy.newProxyInstance(
      classOf[ParserInterface].getClassLoader,
      Array(classOf[ParserInterface]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          if (method.getName == "parsePlan" && args(0).toString.startsWith("FABRIC PROBE "))
            FabricCommandProbeCommand(args(0).toString.endsWith("FAIL"), args(0).toString.endsWith("WAIT"))
          else
            try method.invoke(delegate, args: _*)
            catch { case error: InvocationTargetException => throw error.getCause }
      }
    ).asInstanceOf[ParserInterface]
}

case class FabricCommandProbeCommand(fail: Boolean, waitForPoll: Boolean) extends LeafRunnableCommand {
  override def run(spark: SparkSession): Seq[Row] = {
    FabricCommandProbe.thread = Thread.currentThread().getId
    FabricCommandProbe.values = FabricCommandProbe.keys.map(spark.sparkContext.getLocalProperty)
    if (waitForPoll) {
      FabricCommandProbe.entered.countDown()
      require(FabricCommandProbe.proceed.await(30, TimeUnit.SECONDS), "Fabric poller did not release the action")
    }
    if (fail) throw new IllegalStateException("fabric probe failure")
    Seq.empty
  }
}

class FabricCommandBridgeSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val root = new File(s"target/fabric-bridge-${UUID.randomUUID().toString.take(8)}").getAbsoluteFile
  private val mapper = new ObjectMapper()

  override def beforeAll(): Unit = {
    super.beforeAll()
    root.mkdirs()
    spark = SparkSession.builder().master("local[1]").appName("fabric-command-bridge")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", new File(root, "warehouse").getAbsolutePath)
      .config(FeatureGate.QueryLogEnabledKey, "true")
      .withExtensions(_.injectParser((_, parser) => FabricCommandProbe.parser(parser)))
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      val paths = Files.walk(root.toPath)
      try paths.iterator().asScala.toVector.reverse.foreach(Files.delete)
      finally paths.close()
    } finally super.afterAll()
  }

  private def execute(id: String, sql: String, capture: Boolean = true): FabricCommandBridge.Result =
    FabricCommandBridge.executeCommand(
      spark, id, "run-1", "campaign-1", "model.test", "materialized_view", "db.target",
      "phase-1", "test title", sql, "group-1", capture
    )

  private def release(id: String, capture: Boolean = true): Unit = {
    OpenIvmInsightsBroker.release(spark, id)
    if (capture) QueryLogExport.release(spark, id)
  }

  it("correlates on the executing thread and restores the complete previous properties") {
    val context = spark.sparkContext
    val original = new Properties()
    original.setProperty("openivm.request_id", "previous")
    original.setProperty("openivm.campaign_id", "old-campaign")
    original.setProperty("spark.scheduler.pool", "old-pool")
    original.setProperty("spark.jobGroup.id", "old-group")
    original.setProperty("unrelated", "keep")
    ScopedSqlTestProperties.set(context, original)
    val id = "fabric-success"
    try {
      val thread = Thread.currentThread().getId
      execute(id, "FABRIC PROBE OK")
      FabricCommandProbe.thread shouldBe thread
      FabricCommandProbe.values shouldBe Seq(
        id, "run-1", "model.test", "campaign-1", id, "phase-1", "group-1", "test title", "true"
      )
      ScopedSqlTestProperties.get(context) shouldBe original
      (ScopedSqlTestProperties.get(context) eq original) shouldBe false
      val page = OpenIvmInsightsBroker.page(spark, id, 0L, 100)
      page.status.requestStatus shouldBe Some("succeeded")
      page.status.terminal shouldBe true
      mapper.readTree(QueryLogExport.snapshotJson(spark, id, 100, 65536)).path("sql_succeeded").asBoolean() shouldBe true
    } finally {
      release(id)
      ScopedSqlTestProperties.set(context, new Properties())
    }
  }

  it("allows independent driver polling while the one-thread bridge action remains running") {
    val id = "fabric-concurrent-poll"
    FabricCommandProbe.entered = new CountDownLatch(1)
    FabricCommandProbe.proceed = new CountDownLatch(1)
    val workers = Executors.newFixedThreadPool(2)
    val execution = workers.submit(new Callable[Long] {
      override def call(): Long = {
        val context = spark.sparkContext
        val previous = ScopedSqlTestProperties.get(context).clone().asInstanceOf[Properties]
        try {
          execute(id, "FABRIC PROBE WAIT")
          Thread.currentThread().getId
        } finally ScopedSqlTestProperties.get(context) shouldBe previous
      }
    })
    try {
      FabricCommandProbe.entered.await(10, TimeUnit.SECONDS) shouldBe true
      val poll = workers.submit(new Callable[(Long, OpenIvmInsightsContract.Page)] {
        override def call(): (Long, OpenIvmInsightsContract.Page) = {
          spark.sparkContext.getLocalProperty("openivm.request_id") shouldBe null
          (Thread.currentThread().getId, OpenIvmInsightsBroker.page(spark, id, 0L, 100))
        }
      })
      val (pollThread, page) = poll.get(10, TimeUnit.SECONDS)
      pollThread should not be FabricCommandProbe.thread
      execution.isDone shouldBe false
      page.status.requestId shouldBe id
      page.status.runId shouldBe Some("run-1")
      page.status.dbtNodeId shouldBe Some("model.test")
      page.status.captureStatus shouldBe OpenIvmInsightsContract.CaptureStatus.Running
      page.status.requestStatus shouldBe None
      page.status.terminal shouldBe false
      page.events should not be empty
      val started = page.events.find(_.eventType == OpenIvmInsightsContract.EventType.RequestStarted).get
      val details = mapper.readTree(started.detailsJson.get)
      details.path("request_id").asText() shouldBe id
      details.path("run_id").asText() shouldBe "run-1"
      details.path("dbt_node_id").asText() shouldBe "model.test"
      FabricCommandProbe.values.take(3) shouldBe Seq(id, "run-1", "model.test")
      FabricCommandProbe.proceed.countDown()
      execution.get(15, TimeUnit.SECONDS) shouldBe FabricCommandProbe.thread
      OpenIvmInsightsBroker.page(spark, id, 0L, 100).status.requestStatus shouldBe Some("succeeded")
    } finally {
      FabricCommandProbe.proceed.countDown()
      try execution.get(15, TimeUnit.SECONDS)
      finally {
        workers.shutdownNow()
        workers.awaitTermination(10, TimeUnit.SECONDS) shouldBe true
        release(id)
      }
    }
  }

  it("returns materialized rows and their Spark schema without enabling query log capture") {
    val id = "fabric-rows"
    try {
      val result = execute(id, "SELECT 42 AS answer", capture = false)
      mapper.readTree(result.getSchemaJson).path("fields").get(0).path("name").asText() shouldBe "answer"
      result.getRows.size() shouldBe 1
      result.getRows.get(0).getInt(0) shouldBe 42
      OpenIvmInsightsBroker.page(spark, id, 0L, 100).status.requestStatus shouldBe Some("succeeded")
    } finally release(id, capture = false)
  }

  it("retains failed captures and restores properties without masking the SQL exception") {
    val context = spark.sparkContext
    context.setLocalProperty("openivm.phase", "before")
    val id = "fabric-fail"
    try {
      intercept[IllegalStateException](execute(id, "FABRIC PROBE FAIL")).getMessage shouldBe "fabric probe failure"
      context.getLocalProperty("openivm.phase") shouldBe "before"
      context.getLocalProperty("openivm.request_id") shouldBe null
      OpenIvmInsightsBroker.page(spark, id, 0L, 100).status.requestStatus shouldBe Some("failed")
      mapper.readTree(QueryLogExport.snapshotJson(spark, id, 100, 65536)).path("sql_succeeded").asBoolean() shouldBe false
    } finally {
      release(id)
      context.setLocalProperty("openivm.phase", null)
    }
  }

  it("marks errors during lazy collection as failures and restores the caller context") {
    val id = "fabric-lazy-failure"
    val fail: () => Int = () => throw new IllegalStateException("lazy failure")
    spark.udf.register("fabric_bridge_fail", fail)
    try {
      intercept[Exception](execute(id, "SELECT fabric_bridge_fail()"))
      spark.sparkContext.getLocalProperty("openivm.request_id") shouldBe null
      OpenIvmInsightsBroker.page(spark, id, 0L, 100).status.requestStatus shouldBe Some("failed")
      mapper.readTree(QueryLogExport.snapshotJson(spark, id, 100, 65536)).path("sql_succeeded").asBoolean() shouldBe false
    } finally release(id)
  }

  it("rejects unsafe identity and duplicate query capture before executing SQL") {
    intercept[IllegalArgumentException] {
      execute("bad' OR 1=1 --", "FABRIC PROBE OK")
    }
    val id = "fabric-duplicate"
    QueryLogExport.begin(spark, id)
    FabricCommandProbe.thread = -1L
    try {
      intercept[IllegalArgumentException](execute(id, "FABRIC PROBE OK"))
      FabricCommandProbe.thread shouldBe -1L
      OpenIvmInsightsBroker.page(spark, id, 0L, 100).status.requestStatus shouldBe Some("failed")
      mapper.readTree(QueryLogExport.snapshotJson(spark, id, 100, 65536)).path("sql_succeeded").isNull shouldBe true
    } finally {
      OpenIvmInsightsBroker.release(spark, id)
      QueryLogExport.end(spark, id, sqlSucceeded = false)
      QueryLogExport.release(spark, id)
    }
  }

  it("reports failed insights when query-log cleanup throws after successful SQL") {
    val id = "fabric-query-cleanup-failure"
    val error = new IllegalStateException("query cleanup failure")
    val context = spark.sparkContext
    val previous = ScopedSqlTestProperties.get(context).clone().asInstanceOf[Properties]
    OpenIvmInsightsBroker.begin(spark, id, "cleanup-run", "model.cleanup")
    QueryLogExport.begin(spark, id)
    try {
      val thrown = intercept[IllegalStateException] {
        FabricCommandBridge.preservingFailure(
          () => FabricCommandBridge.finishLifecycle(
            () => FabricCommandBridge.preservingFailure(
              () => { spark.sql("SELECT 1").collect(); () },
              () => {
                QueryLogExport.end(spark, id, sqlSucceeded = true)
                throw error
              }
            ),
            completed => OpenIvmInsightsBroker.end(spark, id, completed)
          ),
          () => ScopedSqlTestProperties.set(context, previous)
        )
      }
      thrown should be theSameInstanceAs error
      OpenIvmInsightsBroker.page(spark, id, 0L, 100).status.requestStatus shouldBe Some("failed")
      mapper.readTree(QueryLogExport.snapshotJson(spark, id, 100, 65536)).path("sql_succeeded").asBoolean() shouldBe true
      ScopedSqlTestProperties.get(context) shouldBe previous
    } finally {
      release(id)
      ScopedSqlTestProperties.set(context, previous)
    }
  }

  it("preserves a primary error while attempting every cleanup") {
    val primary = new IllegalStateException("primary")
    val end = new IllegalArgumentException("end")
    val restore = new IllegalArgumentException("restore")
    val thrown = intercept[IllegalStateException] {
      FabricCommandBridge.preservingFailure(
        () => FabricCommandBridge.preservingFailure(() => throw primary, () => throw end),
        () => throw restore
      )
    }
    thrown should be theSameInstanceAs primary
    thrown.getSuppressed.toSeq shouldBe Seq(end, restore)
  }
}
