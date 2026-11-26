package org.openivm.spark.plugin.metrics

import com.codahale.metrics.MetricRegistry

import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.openivm.spark.plugin.metrics.conf.OpenIvmMetricsConf
import org.openivm.spark.telemetry.{BoundedTelemetryQueue, BoundedTelemetryRuntime, TelemetryJobListener}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

/** SparkPlugin that exports OpenIVM metrics through Spark's native registry. */
class OpenIvmMetricsPlugin extends SparkPlugin with Logging {
  override def driverPlugin: OpenIvmMetricsDriverPlugin =
    try new OpenIvmMetricsDriverPlugin
    catch {
      case NonFatal(e) =>
        logWarning("failed to create OpenIvmMetricsDriverPlugin", e)
        null
    }

  override def executorPlugin: OpenIvmMetricsExecutorPlugin =
    try new OpenIvmMetricsExecutorPlugin
    catch {
      case NonFatal(e) =>
        logWarning("failed to create OpenIvmMetricsExecutorPlugin", e)
        null
    }
}

object OpenIvmMetricsPlugin extends Logging {
  def registerMetrics(ctx: PluginContext): Unit =
    registerMetrics(ctx.metricRegistry)

  def registerMetrics(registry: MetricRegistry): Unit =
    try {
      OpenIvmMetrics.bindRegistry(registry)
      logInfo("registered OpenIVM metrics")
    } catch {
      case NonFatal(e) => logWarning("failed to register OpenIVM metrics", e)
    }
}

class OpenIvmMetricsDriverPlugin extends DriverPlugin with Logging {
  private var telemetryQueue: Option[BoundedTelemetryQueue]             = None
  private var jobListener: Option[(SparkContext, TelemetryJobListener)] = None

  override def init(sc: SparkContext, ctx: PluginContext): java.util.Map[String, String] = {
    val config = OpenIvmMetricsConf(ctx.conf)
    OpenIvmMetrics.configure(config.enabled)
    if (!config.enabled) logInfo("OpenIVM metrics are not enabled")
    try telemetryQueue = BoundedTelemetryRuntime.start(ctx.conf, sc.hadoopConfiguration)
    catch {
      case NonFatal(_) => logWarning("OpenIVM telemetry queue configuration failed; queue is disabled")
    }
    telemetryQueue.foreach { _ =>
      val tasksEnabled =
        try ctx.conf.getBoolean(TelemetryJobListener.TaskMetricsKey, false)
        catch {
          case _: IllegalArgumentException =>
            logWarning("invalid OpenIVM task metrics flag; task summaries are disabled")
            false
        }
      val listener = new TelemetryJobListener(taskMetricsEnabled = tasksEnabled)
      sc.addSparkListener(listener)
      jobListener = Some(sc -> listener)
    }
    new java.util.HashMap[String, String]
  }

  override def registerMetrics(appId: String, ctx: PluginContext): Unit = {
    jobListener.foreach { case (_, listener) =>
      try listener.registerMetrics(ctx.metricRegistry)
      catch { case NonFatal(_) => logWarning("some OpenIVM telemetry listener metrics could not be registered") }
    }
    if (!BoundedTelemetryRuntime.registerMetrics(ctx.metricRegistry))
      logWarning("some OpenIVM telemetry queue metrics could not be registered")
    if (OpenIvmMetrics.enabled) OpenIvmMetricsPlugin.registerMetrics(ctx)
  }

  override def shutdown(): Unit = {
    jobListener.foreach { case (sc, listener) => sc.removeSparkListener(listener) }
    jobListener = None
    telemetryQueue.foreach { queue =>
      try {
        if (!BoundedTelemetryRuntime.stop(queue, 5000L))
          logWarning("OpenIVM telemetry queue did not drain before shutdown")
      } catch {
        case NonFatal(_) => logWarning("OpenIVM telemetry queue shutdown failed")
      }
    }
    telemetryQueue = None
  }
}

class OpenIvmMetricsExecutorPlugin extends ExecutorPlugin with Logging {
  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    val config = OpenIvmMetricsConf(ctx.conf)
    OpenIvmMetrics.configure(config.enabled)
    if (config.enabled) OpenIvmMetricsPlugin.registerMetrics(ctx)
    else logInfo("OpenIVM metrics are not enabled")
  }
}
