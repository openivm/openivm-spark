import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.spark.Success$;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.openivm.spark.telemetry.TelemetryJobListener;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

/** Synthetic callback replay only: no SparkContext, queue I/O, Spark jobs or cross-clock differences. */
public final class TaskObserverReplay {
  static final int fixtureCount = 128, taskCount = 32, repeat = 48;
  final List<SparkListenerJobStart> starts = new ArrayList<>();
  final List<SparkListenerStageCompleted> stages = new ArrayList<>();
  final List<SparkListenerJobEnd> ends = new ArrayList<>();
  final List<List<SparkListenerTaskEnd>> tasks = new ArrayList<>();
  long lines, chars;
  final SparkListener disabled = new SparkListener() {};
  final SparkListener enabled = new TelemetryJobListener(s -> {
    lines++;
    chars += s.length();
    return BoxedUnit.UNIT;
  });

  static <T> Seq<T> seq(List<T> l) { return CollectionConverters.asScala(l).toSeq(); }

  TaskObserverReplay() {
    Properties props = new Properties();
    props.setProperty("openivm.telemetry.action", "fixture/1/0/statement");
    props.setProperty("spark.sql.execution.id", "1");
    TaskMetrics m = TaskMetrics.empty();
    for (int j = 0; j < fixtureCount; j++) {
      long base = 1_000_000L + j * 100;
      StageInfo stage = new StageInfo(j, 0, "fixture", taskCount, seq(List.of()), seq(List.of()), "",
          m, seq(List.of()), Option.empty(), 0, false, 0);
      stage.submissionTime_$eq(Option.apply(base + 1));
      stage.completionTime_$eq(Option.apply(base + 80));
      starts.add(new SparkListenerJobStart(j, base, seq(List.of(stage)), props));
      stages.add(new SparkListenerStageCompleted(stage));
      ends.add(new SparkListenerJobEnd(j, base + 81, JobSucceeded$.MODULE$));
      List<SparkListenerTaskEnd> group = new ArrayList<>();
      for (int i = 0; i < taskCount; i++) {
        TaskInfo t = new TaskInfo((long) j * taskCount + i, i, 0, i, base + 2, "1", "fixture",
            TaskLocality$.MODULE$.PROCESS_LOCAL(), false);
        t.finishTime_$eq(base + 10 + i);
        group.add(new SparkListenerTaskEnd(j, 0, "ResultTask", Success$.MODULE$, t, null, m));
      }
      tasks.add(group);
    }
  }

  long replay(SparkListener listener, int loops) {
    long before = System.nanoTime();
    for (int r = 0; r < loops; r++) {
      for (int j = 0; j < fixtureCount; j++) {
        listener.onJobStart(starts.get(j));
        for (SparkListenerTaskEnd task : tasks.get(j)) listener.onTaskEnd(task);
        listener.onStageCompleted(stages.get(j));
        listener.onJobEnd(ends.get(j));
      }
    }
    return System.nanoTime() - before;
  }

  public static void main(String[] args) {
    TaskObserverReplay p = new TaskObserverReplay();
    for (int i = 0; i < 24; i++) {
      p.replay(p.disabled, repeat);
      p.replay(p.enabled, repeat);
    }
    p.lines = p.chars = 0;
    for (int pair = 0; pair < 6; pair++) {
      long off, on;
      if (pair % 2 == 0) {
        off = p.replay(p.disabled, repeat);
        on = p.replay(p.enabled, repeat);
      } else {
        on = p.replay(p.enabled, repeat);
        off = p.replay(p.disabled, repeat);
      }
      System.out.printf("pair=%d order=%s groups=%d task_events=%d noop_ns=%d listener_ns=%d%n",
          pair + 1, pair % 2 == 0 ? "AB" : "BA", fixtureCount * repeat,
          fixtureCount * repeat * taskCount, off, on);
    }
    System.out.printf("records=%d chars=%d%n", p.lines, p.chars);
    if (p.lines != (long) 6 * fixtureCount * repeat * 3 || p.chars <= 0) throw new AssertionError();
  }
}
