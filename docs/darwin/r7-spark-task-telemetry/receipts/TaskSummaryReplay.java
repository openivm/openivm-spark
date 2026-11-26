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

/** Replays the same submitted stage and physical task attempts into two real listeners; no Spark jobs or queue I/O. */
public class TaskSummaryReplay {
  static final class Arm {
    long lines, chars;
    final TelemetryJobListener listener;
    Arm(boolean detailed) {
      listener = new TelemetryJobListener(s -> { lines++; chars += s.length(); return BoxedUnit.UNIT; }, detailed, 512);
    }
  }
  static <T> Seq<T> seq(List<T> l) { return CollectionConverters.asScala(l).toSeq(); }
  final SparkListenerJobStart start;
  final SparkListenerStageSubmitted submit;
  final SparkListenerStageCompleted stageEnd;
  final SparkListenerJobEnd end = new SparkListenerJobEnd(0, 100000L, JobSucceeded$.MODULE$);
  final SparkListenerTaskEnd[] tasks;

  TaskSummaryReplay(int count) {
    TaskMetrics m = TaskMetrics.empty();
    m.setExecutorRunTime(12L);
    m.setExecutorCpuTime(73456L);
    m.inputMetrics().incRecordsRead(32L);
    m.inputMetrics().incBytesRead(128L);
    StageInfo stage = new StageInfo(0, 0, "fixture", count, seq(List.of()), seq(List.of()), "",
        m, seq(List.of()), Option.empty(), 0, false, 0);
    stage.submissionTime_$eq(Option.apply(1000L));
    stage.completionTime_$eq(Option.apply(100000L));
    Properties props = new Properties();
    props.setProperty("openivm.telemetry.action", "fixture/1/0/statement");
    props.setProperty("openivm.telemetry.refresh_id", "fixture-op");
    props.setProperty("openivm.request_id", "fixture-req");
    props.setProperty("spark.sql.execution.id", "1");
    start = new SparkListenerJobStart(0, 995L, seq(List.of(stage)), props);
    submit = new SparkListenerStageSubmitted(stage, props);
    stageEnd = new SparkListenerStageCompleted(stage);
    tasks = new SparkListenerTaskEnd[count];
    for (int i = 0; i < count; i++) {
      TaskInfo t = new TaskInfo(i, i, 0, i, 1024L, "1", "fixture", TaskLocality$.MODULE$.PROCESS_LOCAL(), false);
      t.finishTime_$eq(1100L + i);
      tasks[i] = new SparkListenerTaskEnd(0, 0, "ResultTask", Success$.MODULE$, t, null, m);
    }
  }

  long replay(Arm arm, int groups) {
    long t = System.nanoTime();
    for (int i = 0; i < groups; i++) {
      arm.listener.onJobStart(start);
      arm.listener.onStageSubmitted(submit);
      for (SparkListenerTaskEnd task : tasks) arm.listener.onTaskEnd(task);
      arm.listener.onStageCompleted(stageEnd);
      arm.listener.onJobEnd(end);
    }
    return System.nanoTime() - t;
  }

  public static void main(String[] args) {
    for (int count : new int[]{32, 1024}) {
      TaskSummaryReplay probe = new TaskSummaryReplay(count);
      Arm legacy = new Arm(false), deep = new Arm(true);
      int groups = 131072/count;
      for (int i = 0; i < 16; i++) { probe.replay(legacy, groups); probe.replay(deep, groups); }
      legacy.chars = legacy.lines = deep.chars = deep.lines = 0;
      for (int i = 0; i < 6; i++) {
        long a, b;
        if (i%2 == 0) { a = probe.replay(legacy,groups); b = probe.replay(deep,groups); }
        else { b = probe.replay(deep,groups); a = probe.replay(legacy,groups); }
        System.out.printf("tasks_per_stage=%d pair=%d order=%s groups=%d task_events=%d legacy_ns=%d deep_ns=%d%n",
            count, i+1, i%2 == 0 ? "AB" : "BA", groups, groups*count, a, b);
      }
      if (legacy.lines != 18L * groups || legacy.lines != deep.lines || deep.chars <= legacy.chars)
        throw new AssertionError("records do not reconcile");
      System.out.printf("tasks_per_stage=%d records_per_arm=%d legacy_chars=%d deep_chars=%d%n",
          count, legacy.lines, legacy.chars, deep.chars);
    }
  }
}
