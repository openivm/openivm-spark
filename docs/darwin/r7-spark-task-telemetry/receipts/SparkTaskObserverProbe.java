import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openivm.spark.telemetry.BoundedTelemetryQueue;
import org.openivm.spark.telemetry.BoundedTelemetryRuntime;

/** Separate JVM per arm. Uses common's tested Spark 4.1 classpath and an isolated local filesystem. */
public class SparkTaskObserverProbe {
  static final long N = 32768;
  static final int WARM = 4, RUNS = 6;

  static void run(SparkSession spark) {
    List<Row> rows = spark.sql("SELECT id % 31 k, count(*) n FROM range(0," + N +
        ",1,8) GROUP BY id % 31").collectAsList();
    int total = 0;
    for (Row r : rows) {
      long k = r.getLong(0), n = r.getLong(1);
      if (k < 0 || k >= 31 || n != N / 31 + (k < N % 31 ? 1 : 0)) throw new AssertionError(r);
      total += n;
    }
    if (rows.size() != 31 || total != N) throw new AssertionError();
  }

  static void drain(SparkContext sc, boolean enabled) throws Exception {
    sc.listenerBus().waitUntilEmpty(30000L);
    if (enabled && !BoundedTelemetryRuntime.awaitDrain(30000L)) throw new AssertionError("queue drain");
  }

  public static void main(String[] args) throws Exception {
    String arm = args[0];
    boolean enabled = !arm.equals("off"), deep = arm.equals("deep");
    Path root = Files.createTempDirectory("r7-s3-real-" + arm);
    Path events = root.resolve("events");
    SparkSession spark = SparkSession.builder().master("local[2]").appName("SparkTaskObserverProbe")
        .config("spark.plugins", "org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin")
        .config("spark.openivm.telemetry.queue.enabled", enabled)
        .config("spark.openivm.telemetry.taskMetrics.enabled", deep)
        .config("spark.openivm.telemetry.queue.localUri", events.toUri().toString())
        .config("spark.sql.adaptive.enabled", false).config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.warehouse.dir", root.resolve("warehouse").toUri().toString())
        .config("spark.ui.enabled", false).config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate();
    try {
      SparkContext sc = spark.sparkContext();
      sc.setLogLevel("ERROR");
      sc.setLocalProperty("openivm.telemetry.action", "warm");
      sc.setLocalProperty("openivm.request_id", "observer-probe");
      sc.setLocalProperty("openivm.telemetry.refresh_id", "observer-probe-op");
      for (int i = 0; i < WARM; i++) run(spark);
      drain(sc, enabled);
      long publishedBefore = enabled ? BoundedTelemetryRuntime.health().get().published() : 0L;
      sc.setLocalProperty("openivm.telemetry.action", "measured");
      long start = System.nanoTime();
      for (int i = 0; i < RUNS; i++) run(spark);
      long returned = System.nanoTime();
      drain(sc, enabled);
      long drained = System.nanoTime();
      long published = 0, drops = 0, failures = 0, jobs = 0, stages = 0, tasks = 0, read = 0, write = 0;
      if (enabled) {
        BoundedTelemetryQueue.Health h = BoundedTelemetryRuntime.health().get();
        published = h.published() - publishedBefore;
        drops = h.droppedOversized() + h.droppedFull() + h.droppedContended() + h.droppedClosed();
        failures = h.workerFailures() + h.manifestFailures() + h.exportFailures();
        ObjectMapper mapper = new ObjectMapper();
        try (Stream<Path> files = Files.list(events)) {
          for (Path file : files.filter(f -> f.toString().endsWith(".jsonl")).toList()) {
            JsonNode event = mapper.readTree(Files.readString(file));
            if (event.path("event_type").asText().equals("spark_job_start")) jobs++;
            if (event.path("event_type").asText().equals("spark_stage_completed")) {
              stages++;
              JsonNode sum = event.get("task_summary");
              if (deep != (sum != null)) throw new AssertionError("summary");
              if (deep) {
                if (!event.path("task_summary_status").asText().equals("observed") ||
                    sum.path("attempts_without_metrics").asLong() != 0) throw new AssertionError("carriers");
                tasks += sum.path("attempts_with_metrics").asLong();
                read += sum.path("shuffle_read_records").asLong();
                write += sum.path("shuffle_write_records").asLong();
              }
            }
          }
        }
        if (published != RUNS * 4L || jobs != RUNS + WARM || stages != 2 * jobs ||
            (deep && (tasks != 12 * jobs || read <= 0 || read != write)) || drops != 0 || failures != 0)
          throw new AssertionError("unexpected physical inventory jobs=" + jobs + " stages=" + stages +
              " tasks=" + tasks + " published=" + published + " read=" + read + " write=" + write);
      }
      System.out.printf("MEASURE arm=%s return_ns=%d drained_ns=%d tail_ns=%d measured_records=%d drops=%d failures=%d all_jobs=%d all_stages=%d all_tasks_with_metrics=%d all_shuffle_records_read=%d all_shuffle_records_written=%d events=%s%n",
          arm, returned-start, drained-start, drained-returned, published, drops, failures, jobs, stages,
          tasks, read, write, events);
    } finally { spark.stop(); }
  }
}
