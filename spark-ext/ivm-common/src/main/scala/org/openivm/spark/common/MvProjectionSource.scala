package org.openivm.spark.common

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/** Row-local public projections commute with signed changes to their backing
  * Delta table. HAVING/Top-K views are deliberately not marked with this contract.
  */
object MvProjectionSource {
  val CatalogProperty: String = "_ivm_public_projection_v1"

  def isProjection(view: View): Boolean =
    !view.isTempView && view.desc.properties.get(CatalogProperty).contains("true")

  def projection(plan: LogicalPlan): Option[View] = plan match {
    case alias: SubqueryAlias             => projection(alias.child)
    case view: View if isProjection(view) => Some(view)
    case _                                => None
  }

  def projectionView(spark: SparkSession, source: String): Option[View] =
    try projection(spark.table(source).queryExecution.analyzed)
    catch { case _: AnalysisException => None }

  def isProjection(spark: SparkSession, source: String): Boolean = projectionView(spark, source).nonEmpty

  def outputNames(spark: SparkSession, source: String): PublicOutputNames =
    projectionView(spark, source).fold(PublicOutputNames.Empty)(view => PublicOutputNames.of(view.desc.properties))

  /** Backing columns carry the view's [[PublicOutputNames]] physical names; rows staged through the public
    * schema keep public names, so a missing physical name falls back to the public one.
    */
  def project(
      raw: DataFrame,
      schema: StructType,
      metadataColumns: Seq[String] = Nil,
      names: PublicOutputNames = PublicOutputNames.Empty
  ): DataFrame = {
    val columns = schema.fields.map { field =>
      val physicalMatches =
        names.physicalFor(field.name).toSeq.flatMap(name => raw.schema.fields.filter(_.name.equalsIgnoreCase(name)))
      val matches =
        if (physicalMatches.nonEmpty) physicalMatches
        else raw.schema.fields.filter(_.name.equalsIgnoreCase(field.name)).toSeq
      require(matches.length == 1, s"Public MV column '${field.name}' did not resolve uniquely on backing state")
      val physical = matches.head
      val value    = col(quote(physical.name))
      val typed    = if (physical.dataType == field.dataType) value else value.cast(field.dataType)
      typed.as(field.name, field.metadata)
    }
    raw.select((columns.toSeq ++ metadataColumns.map(name => col(quote(name)))): _*)
  }

  /** Delta's DataFrameReader cannot time-travel or read CDF through a Spark VIEW.
    * Resolve the same backing log, then reapply the persisted public schema.
    * This reads catalog/Delta metadata only; it never opens OpenIVM state.
    */
  def read(spark: SparkSession, source: String, options: Map[String, String]): DataFrame =
    readProjection(spark, spark.table(source), options)
      .getOrElse(spark.read.format("delta").options(options).table(source))

  def readProjection(spark: SparkSession, logical: DataFrame, options: Map[String, String]): Option[DataFrame] = {
    val plan = logical.queryExecution.analyzed
    projection(plan).map { view =>
      val log = DeltaTableVersion.deltaLogFromPlan(view.child).filter(_.tableExists).getOrElse {
        throw new IllegalStateException(
          s"Public MV projection '${view.desc.identifier}' has no committed Delta backing"
        )
      }
      val raw = spark.read.format("delta").options(options).load(log.dataPath.toString)
      val metadata =
        if (options.get("readChangeFeed").exists(_.equalsIgnoreCase("true")))
          Seq("_change_type", "_commit_version", "_commit_timestamp")
        else Nil
      project(raw, logical.schema, metadata, PublicOutputNames.of(view.desc.properties))
    }
  }

  private def quote(name: String): String = s"`${name.replace("`", "``")}`"
}
