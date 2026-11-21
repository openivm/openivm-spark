package org.openivm.spark.commands

import org.apache.spark.sql.SparkSession
import org.openivm.spark.insights.OpenIvmInsightJson
import org.openivm.spark.telemetry.TelemetryJobListener

private[commands] object GeneratedActionOwnership {
  val Property = "openivm.generated.action"

  def withTrace[A](spark: SparkSession, action: Option[String], boundary: String)(run: => A): A =
    action match {
      case None => run
      case Some(value) =>
        val context = spark.sparkContext
        val previous = context.getLocalProperty(TelemetryJobListener.ActionProperty)
        context.setLocalProperty(
          TelemetryJobListener.ActionProperty,
          OpenIvmInsightJson.obj("action" -> value, "boundary" -> boundary)
        )
        try run
        finally context.setLocalProperty(TelemetryJobListener.ActionProperty, previous)
    }

  // JobStart inherits ownership, not a classification: Delta may submit log/state jobs here too.
  def withBoundary[A](spark: SparkSession, owner: Option[String], boundary: String)(run: => A): A =
    owner match {
      case None => run
      case Some(value) =>
        val context  = spark.sparkContext
        val previous = context.getLocalProperty(Property)
        context.setLocalProperty(Property, s"$value;boundary=$boundary")
        try run
        finally context.setLocalProperty(Property, previous)
    }
}
