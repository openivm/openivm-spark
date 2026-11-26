import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openivm.spark.common.CdfWatermarkCatalog;
import org.openivm.spark.common.FabricCommandBridge;
import org.openivm.spark.common.MvCatalog;
import org.openivm.spark.common.QueryLogExport;
import org.openivm.spark.common.StagingCatalog;
import org.openivm.spark.insights.OpenIvmInsightsBroker;
import org.openivm.spark.telemetry.BoundedTelemetryQueue;
import org.openivm.spark.telemetry.BoundedTelemetryRuntime;

/**
 * Local replica of the D7 six-MV CDF CREATE/REFRESH protocol, one fresh JVM per observer arm.
 * Arms: off (queue disabled), legacy (queue on, taskMetrics off), deep (queue on, taskMetrics on).
 * Every command runs through FabricCommandBridge with a unique request/job group. Each future also pages
 * SHOW OPENIVM INSIGHTS, calls awaitDrain(10000) and releases its captures. Correctness and telemetry
 * inventory run outside timing.
 */
public class CdfObserverProbe {
  static final int N = 6;
  static final Pattern EFFECTIVE = Pattern.compile("\"effective_refresh_type\"\\s*:\\s*\"([A-Z_]+)\"");
  static final ObjectMapper M = new ObjectMapper();
  static final AtomicLong RELEASE_RETRIES = new AtomicLong();

  /** Constant-cost denominators present in every arm. */
  static final class Census extends SparkListener {
    final AtomicLong jobs = new AtomicLong(), jobsWithRequest = new AtomicLong(), jobsWithGroup = new AtomicLong();
    final AtomicLong tasks = new AtomicLong(), tasksWithMetrics = new AtomicLong();
    final ConcurrentHashMap<String, AtomicLong> unattributed = new ConcurrentHashMap<>();
    @Override public void onJobStart(SparkListenerJobStart e) {
      jobs.incrementAndGet();
      Properties p = e.properties();
      if (p != null && p.getProperty("openivm.request_id") != null) jobsWithRequest.incrementAndGet();
      else {
        String d = p == null ? null : p.getProperty("spark.job.description");
        if (d == null && p != null) d = p.getProperty("callSite.short");
        String key = d == null ? "<none>" : d.replaceAll("\\s+", " ");
        if (key.length() > 100) key = key.substring(0, 100);
        if (!unattributed.containsKey(key) && unattributed.size() >= 48) key = "<overflow>";
        unattributed.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
      }
      if (p != null && p.getProperty("spark.jobGroup.id") != null) jobsWithGroup.incrementAndGet();
    }
    @Override public void onTaskEnd(SparkListenerTaskEnd e) {
      tasks.incrementAndGet();
      if (e.taskMetrics() != null) tasksWithMetrics.incrementAndGet();
    }
  }

  record Cmd(String requestId, String mv, long startNs, long returnNs, long captureNs, long doneNs,
             boolean drained, Set<String> effective, int insightRows) {}

  static Cmd command(SparkSession spark, String runId, String db, String phase, int i, String sql, long t0)
      throws Exception {
    String mv = db + ".concurrent_" + i;
    String rid = runId + "-" + phase + "-" + i;
    long s = System.nanoTime();
    FabricCommandBridge.executeCommand(spark, rid, runId, "r7-s3-cdf", "model.concurrent_" + i,
        "materialized_view", mv, phase, phase + " " + mv, sql, rid, true);
    long r = System.nanoTime();
    Set<String> effective = new TreeSet<>();
    long after = 0; int rows = 0; boolean more = true;
    while (more) {
      List<Row> page = spark.sql("SHOW OPENIVM INSIGHTS FOR REQUEST '" + rid + "' AFTER " + after +
          " LIMIT 500").collectAsList();
      more = false;
      for (Row row : page) {
        rows++;
        if ("status".equals(row.getAs("record_type"))) {
          after = ((Number) row.getAs("next_sequence")).longValue();
          more = Boolean.TRUE.equals(row.getAs("has_more"));
        } else {
          Object details = row.getAs("details_json");
          if (details != null) {
            Matcher m = EFFECTIVE.matcher(details.toString());
            while (m.find()) effective.add(m.group(1));
          }
        }
      }
    }
    long c = System.nanoTime();
    boolean drained = BoundedTelemetryRuntime.awaitDrain(10000L);
    OpenIvmInsightsBroker.release(spark, rid);
    long deadline = System.nanoTime() + 60_000_000_000L;
    while (true) {
      try {
        QueryLogExport.release(spark, rid);
        break;
      } catch (IllegalArgumentException e) {
        String msg = String.valueOf(e.getMessage());
        if (!(msg.contains("CAPTURE_NOT_TERMINAL") || msg.contains("CAPTURE_BUSY")) || System.nanoTime() > deadline)
          throw e;
        RELEASE_RETRIES.incrementAndGet();
        Thread.sleep(5);
      }
    }
    long d = System.nanoTime();
    return new Cmd(rid, mv, s - t0, r - t0, c - t0, d - t0, drained, effective, rows);
  }

  static ObjectNode batch(SparkSession spark, String runId, String db, String phase, String src, int workers)
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      List<Future<Cmd>> futures = new ArrayList<>();
      long t0 = System.nanoTime();
      for (int i = 1; i <= N; i++) {
        final int k = i;
        String sql = phase.startsWith("create")
            ? "CREATE MATERIALIZED VIEW " + db + ".concurrent_" + k + " AS SELECT symbol, COUNT(*) AS events, " +
              "SUM(amount * " + k + ") AS total FROM " + db + "." + src + " GROUP BY symbol"
            : "REFRESH MATERIALIZED VIEW " + db + ".concurrent_" + k;
        futures.add(pool.submit(() -> command(spark, runId, db, phase, k, sql, t0)));
      }
      List<Cmd> cmds = new ArrayList<>();
      for (Future<Cmd> f : futures) cmds.add(f.get());
      long end = System.nanoTime() - t0;
      ObjectNode b = M.createObjectNode();
      b.put("phase", phase).put("source", src).put("workers", workers).put("batch_ns", end);
      b.put("last_return_ns", cmds.stream().mapToLong(Cmd::returnNs).max().getAsLong());
      b.put("command_sum_ns", cmds.stream().mapToLong(c -> c.returnNs() - c.startNs()).sum());
      b.put("drain_timeouts", cmds.stream().filter(c -> !c.drained()).count());
      ArrayNode arr = b.putArray("commands");
      for (Cmd c : cmds) {
        ObjectNode o = arr.addObject();
        o.put("request_id", c.requestId()).put("mv", c.mv()).put("start_ns", c.startNs())
            .put("return_ns", c.returnNs()).put("capture_ns", c.captureNs()).put("done_ns", c.doneNs())
            .put("drained", c.drained()).put("insight_rows", c.insightRows());
        ArrayNode e = o.putArray("effective_refresh_types");
        c.effective().forEach(e::add);
      }
      return b;
    } finally {
      pool.shutdown();
    }
  }

  static void parity(SparkSession spark, String db, String src) {
    for (int i = 1; i <= N; i++) {
      var mv = spark.sql("SELECT symbol, events, total FROM " + db + ".concurrent_" + i);
      var ex = spark.sql("SELECT symbol, COUNT(*) AS events, SUM(amount * " + i + ") AS total FROM " + db + "." +
          src + " GROUP BY symbol");
      long a = mv.exceptAll(ex).count(), b = ex.exceptAll(mv).count();
      if (a != 0 || b != 0) throw new AssertionError("parity " + db + ".concurrent_" + i + " " + a + "/" + b);
    }
  }

  static void requireIncremental(ObjectNode b) {
    for (JsonNode c : b.get("commands")) {
      Set<String> types = new TreeSet<>();
      c.get("effective_refresh_types").forEach(t -> types.add(t.asText()));
      if (types.isEmpty() || types.contains("FULL_REFRESH") || types.contains("COMPILE_FAILED"))
        throw new AssertionError("non-incremental " + c.get("request_id") + " " + types);
    }
  }

  static ObjectNode cycle(SparkSession spark, String runId, String db) throws Exception {
    spark.sql("CREATE DATABASE IF NOT EXISTS " + db);
    for (String src : List.of("serial_events", "parallel_events")) {
      spark.sql("CREATE TABLE " + db + "." + src + " (id BIGINT, symbol STRING, amount BIGINT) USING DELTA " +
          "TBLPROPERTIES (delta.enableChangeDataFeed = true)");
      spark.sql("INSERT INTO " + db + "." + src + " VALUES (1,'AAA',10),(2,'BBB',20),(3,'CCC',30)");
    }
    ObjectNode out = M.createObjectNode();
    ArrayNode batches = out.putArray("batches");
    ObjectNode c1 = batch(spark, runId, db, "create_serial_source", "serial_events", N);
    parity(spark, db, "serial_events");
    spark.sql("INSERT INTO " + db + ".serial_events VALUES (101,'AAA',7),(102,'BBB',11),(103,'CCC',13)");
    ObjectNode r1 = batch(spark, runId, db, "refresh_serial", "serial_events", 1);
    parity(spark, db, "serial_events");
    requireIncremental(r1);
    for (int i = 1; i <= N; i++) spark.sql("DROP MATERIALIZED VIEW " + db + ".concurrent_" + i);
    ObjectNode c2 = batch(spark, runId, db, "create_parallel_source", "parallel_events", N);
    parity(spark, db, "parallel_events");
    spark.sql("INSERT INTO " + db + ".parallel_events VALUES (101,'AAA',7),(102,'BBB',11),(103,'CCC',13)");
    ObjectNode r2 = batch(spark, runId, db, "refresh_parallel", "parallel_events", N);
    parity(spark, db, "parallel_events");
    requireIncremental(r2);
    batches.add(c1).add(r1).add(c2).add(r2);
    return out;
  }

  static ObjectNode inventory(Path events, String measuredPrefix) throws Exception {
    ObjectNode inv = M.createObjectNode();
    long files = 0, bytes = 0, lines = 0, jobs = 0, jobsReq = 0, jobsOp = 0, jobsAction = 0, measuredJobs = 0;
    long measuredRefreshJobs = 0, measuredRefreshJobsOp = 0, truncatedJobs = 0, stages = 0, stagesWithSummary = 0;
    long duplicateStageAttempts = 0;
    Map<String, Long> types = new TreeMap<>(), status = new TreeMap<>(), sums = new TreeMap<>();
    Set<String> stageKeys = new HashSet<>();
    if (Files.exists(events)) {
      try (Stream<Path> walk = Files.walk(events)) {
        for (Path f : walk.filter(p -> p.toString().endsWith(".jsonl")).toList()) {
          files++;
          bytes += Files.size(f);
          for (String line : Files.readAllLines(f)) {
            if (line.isBlank()) continue;
            lines++;
            JsonNode e = M.readTree(line);
            String type = e.path("event_type").asText("");
            if (type.isEmpty()) {
              for (String alt : new String[] {"record_type", "type", "kind"})
                if (e.hasNonNull(alt)) { type = "~" + alt + "=" + e.get(alt).asText(); break; }
              if (type.isEmpty()) type = "~keys=" + String.join(",", (Iterable<String>) e::fieldNames);
            }
            types.merge(type, 1L, Long::sum);
            if (type.equals("spark_job_start")) {
              jobs++;
              String rid = e.path("request_id").asText(null);
              if (rid != null) jobsReq++;
              if (e.has("operation_id")) jobsOp++;
              if (e.has("action_context")) jobsAction++;
              if (e.path("stage_tracking_truncated").asBoolean(false)) truncatedJobs++;
              if (rid != null && rid.startsWith(measuredPrefix)) {
                measuredJobs++;
                if (rid.contains("-refresh_")) {
                  measuredRefreshJobs++;
                  if (e.has("operation_id")) measuredRefreshJobsOp++;
                }
              }
            } else if (type.equals("spark_stage_completed")) {
              stages++;
              if (!stageKeys.add(e.path("stage_id").asText() + ":" + e.path("stage_attempt").asText()))
                duplicateStageAttempts++;
              if (e.has("task_summary_status")) status.merge(e.get("task_summary_status").asText(), 1L, Long::sum);
              JsonNode s = e.get("task_summary");
              if (s != null) {
                stagesWithSummary++;
                s.fields().forEachRemaining(x -> {
                  String k = x.getKey();
                  if (!x.getValue().isIntegralNumber() || k.endsWith("_epoch_ms")) return;
                  sums.merge(k, x.getValue().asLong(), k.endsWith("_max_ms") ? Math::max : Long::sum);
                });
              }
            }
          }
        }
      }
    }
    inv.put("jsonl_files", files).put("jsonl_bytes", bytes).put("records", lines).put("job_starts", jobs)
        .put("job_starts_with_request_id", jobsReq).put("job_starts_with_operation_id", jobsOp)
        .put("job_starts_with_action", jobsAction).put("job_starts_stage_truncated", truncatedJobs)
        .put("measured_job_starts", measuredJobs).put("measured_refresh_job_starts", measuredRefreshJobs)
        .put("measured_refresh_job_starts_with_operation_id", measuredRefreshJobsOp)
        .put("stage_completed", stages).put("stage_completed_with_summary", stagesWithSummary)
        .put("duplicate_stage_attempt_records", duplicateStageAttempts);
    inv.set("event_types", M.valueToTree(types));
    inv.set("task_summary_status", M.valueToTree(status));
    inv.set("task_summary_aggregates_all_cycles_sum_or_max", M.valueToTree(sums));
    return inv;
  }

  public static void main(String[] args) throws Exception {
    String arm = args[0], label = args[1];
    boolean enabled = !arm.equals("off"), deep = arm.equals("deep");
    Path root = Files.createTempDirectory("r7-s3-cdf-" + label + "-" + arm + "-");
    Path events = root.resolve("live");
    SparkSession spark = SparkSession.builder().master("local[4]").appName("CdfObserverProbe")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.openivm.spark.OpenIvmSparkExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.plugins", "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin")
        .config("spark.native.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.openivm.enabled", "true")
        .config("spark.openivm.changeFeed.mode", "cdf")
        .config("spark.openivm.catalog.backend", "delta")
        .config("spark.openivm.catalog.path", root.resolve("catalog").toUri().toString())
        .config("spark.openivm.profile.refresh", "true")
        .config("spark.openivm.queryLog.enabled", "true")
        .config("spark.openivm.refresh.windowRunningIncremental.enabled", "true")
        .config("spark.openivm.statePath", root.resolve("state").toString())
        .config("spark.openivm.stateSync.async.enabled", "false")
        .config("spark.openivm.stateSync.uri", root.resolve("state-remote").toUri().toString())
        .config("spark.openivm.telemetry.queue.enabled", Boolean.toString(enabled))
        .config("spark.openivm.telemetry.taskMetrics.enabled", Boolean.toString(deep))
        .config("spark.openivm.telemetry.queue.localUri", events.toUri().toString())
        .config("spark.openivm.telemetry.uri", root.resolve("telemetry").toUri().toString())
        .config("spark.sql.warehouse.dir", root.resolve("warehouse").toUri().toString())
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost").config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate();
    Census census = new Census();
    try {
      SparkContext sc = spark.sparkContext();
      sc.setLogLevel("ERROR");
      sc.addSparkListener(census);
      MvCatalog.ensureTables(spark);
      StagingCatalog.ensureTables(spark);
      CdfWatermarkCatalog.ensureTables(spark);
      String runId = "r7s3-" + label + "-" + arm;
      cycle(spark, runId + "-warm", "openivm_warm");
      sc.listenerBus().waitUntilEmpty(60000L);
      BoundedTelemetryRuntime.awaitDrain(30000L);
      long jobsBefore = census.jobs.get(), tasksBefore = census.tasks.get();
      long reqBefore = census.jobsWithRequest.get();
      long publishedBefore = enabled ? BoundedTelemetryRuntime.health().get().published() : 0L;
      census.unattributed.clear();
      ObjectNode measured = cycle(spark, runId + "-m", "openivm_demo");
      sc.listenerBus().waitUntilEmpty(60000L);
      boolean finalDrain = BoundedTelemetryRuntime.awaitDrain(30000L);
      ObjectNode result = M.createObjectNode();
      result.put("arm", arm).put("label", label).put("root", root.toString()).put("final_drain", finalDrain);
      result.put("spark_version", spark.version()).put("java_version", System.getProperty("java.version"));
      result.put("querylog_release_retries_all_cycles", RELEASE_RETRIES.get());
      result.set("measured", measured);
      ObjectNode c = result.putObject("census");
      c.put("measured_jobs", census.jobs.get() - jobsBefore)
          .put("measured_jobs_with_request_property", census.jobsWithRequest.get() - reqBefore)
          .put("measured_tasks", census.tasks.get() - tasksBefore)
          .put("all_jobs", census.jobs.get()).put("all_jobs_with_request_property", census.jobsWithRequest.get())
          .put("all_jobs_with_job_group", census.jobsWithGroup.get())
          .put("all_tasks", census.tasks.get()).put("all_tasks_with_metrics", census.tasksWithMetrics.get());
      ObjectNode un = c.putObject("measured_jobs_without_request_by_description");
      new TreeMap<>(census.unattributed).forEach((k, v) -> un.put(k, v.get()));
      if (enabled) {
        BoundedTelemetryQueue.Health h = BoundedTelemetryRuntime.health().get();
        ObjectNode q = result.putObject("queue");
        q.put("measured_published", h.published() - publishedBefore).put("published", h.published())
            .put("accepted", h.accepted()).put("dropped_full", h.droppedFull())
            .put("dropped_oversized", h.droppedOversized()).put("dropped_closed", h.droppedClosed())
            .put("dropped_contended", h.droppedContended()).put("export_failures", h.exportFailures())
            .put("manifest_failures", h.manifestFailures()).put("worker_failures", h.workerFailures());
      }
      result.set("inventory", inventory(events, runId + "-m-"));
      System.out.println("RESULT " + M.writeValueAsString(result));
    } finally {
      spark.stop();
    }
  }
}
