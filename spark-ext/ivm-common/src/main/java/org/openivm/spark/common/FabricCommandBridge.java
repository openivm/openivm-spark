package org.openivm.spark.common;

import java.util.List;
import java.util.Properties;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openivm.spark.insights.OpenIvmInsightsBroker;
import org.openivm.spark.telemetry.OpenIvmTelemetryContract;
import scala.Option;

/** A single driver-thread call for a Fabric JavaGateway command. */
public final class FabricCommandBridge {
    private FabricCommandBridge() {}

    public static int apiVersion() {
        return 1;
    }

    /** The schema is Spark StructType JSON; rows are materialized Spark Row objects. */
    public static final class Result {
        private final String schemaJson;
        private final List<Row> rows;

        private Result(String schemaJson, List<Row> rows) {
            this.schemaJson = schemaJson;
            this.rows = rows;
        }

        public String getSchemaJson() { return schemaJson; }
        public List<Row> getRows() { return rows; }
    }

    /**
     * materialization and targetRelation are either both null or both supplied.
     * The retained insight and optional query-log captures are released separately.
     */
    public static Result executeCommand(
            SparkSession spark, String requestId, String runId, String campaignId,
            String nodeId, String materialization, String targetRelation, String phase,
            String title, String sql, String jobGroupId, boolean captureQueryLog)
            throws InterruptedException {
        require(spark != null, "spark must not be null");
        require(sql != null && !sql.trim().isEmpty(), "sql must be nonempty");
        require(campaignId != null && !campaignId.trim().isEmpty(), "campaignId must be nonempty");
        require(phase != null && !phase.trim().isEmpty(), "phase must be nonempty");
        require(title != null, "title must not be null");
        require(jobGroupId != null && !jobGroupId.isEmpty(), "jobGroupId must be nonempty");
        SparkContext context = spark.sparkContext();
        Properties current = context.getLocalProperties();
        Properties previous = current == null ? null : (Properties) current.clone();
        Result[] result = {null};
        boolean[] sqlSucceeded = {false};
        preservingFailure(() -> {
            context.setLocalProperties(previous == null ? new Properties() : (Properties) previous.clone());
            context.setJobGroup(jobGroupId, title, true);
            context.setLocalProperty(OpenIvmTelemetryContract.CampaignIdProperty(), campaignId);
            context.setLocalProperty(OpenIvmTelemetryContract.PhaseProperty(), phase);
            context.setLocalProperty(OpenIvmTelemetryContract.CorrelationProperty(), requestId);
            // These are the same broker operations used by the INSIGHTS BEGIN/END SQL commands.
            OpenIvmInsightsBroker.begin(spark, requestId, runId, nodeId,
                    Option.apply(materialization), Option.apply(targetRelation));
            finishLifecycle(() -> {
                if (captureQueryLog) {
                    QueryLogExport.begin(spark, requestId);
                }
                preservingFailure(() -> {
                    checkInterrupted();
                    Dataset<Row> dataset = spark.sql(sql);
                    String schema = dataset.schema().json();
                    List<Row> rows = dataset.collectAsList();
                    checkInterrupted();
                    sqlSucceeded[0] = true;
                    result[0] = new Result(schema, rows);
                }, () -> {
                    if (captureQueryLog) QueryLogExport.end(spark, requestId, sqlSucceeded[0]);
                });
            }, completed -> OpenIvmInsightsBroker.end(spark, requestId, completed));
        }, () -> context.setLocalProperties(previous));
        return result[0];
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Fabric command interrupted");
    }

    @FunctionalInterface
    interface Action { void run() throws InterruptedException; }

    @FunctionalInterface
    interface Completion { void finish(boolean completed) throws InterruptedException; }

    /** Completion includes inner cleanup, not just successful SQL materialization. */
    static void finishLifecycle(Action body, Completion cleanup) throws InterruptedException {
        boolean[] completed = {false};
        preservingFailure(() -> {
            body.run();
            completed[0] = true;
        }, () -> cleanup.finish(completed[0]));
    }

    static void preservingFailure(Action body, Action cleanup) throws InterruptedException {
        Throwable primary = null;
        try {
            body.run();
        } catch (Throwable error) {
            primary = error;
            throw error;
        } finally {
            if (primary == null) {
                cleanup.run();
            } else {
                try {
                    cleanup.run();
                } catch (Throwable error) {
                    if (error != primary) primary.addSuppressed(error);
                }
            }
        }
    }
}
