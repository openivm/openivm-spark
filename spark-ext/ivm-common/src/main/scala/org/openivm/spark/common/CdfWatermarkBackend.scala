package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, greatest, lit, when}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

private[common] trait CdfWatermarkBackend {
  def ensureTables(spark: SparkSession): Unit
  def get(spark: SparkSession, viewName: String, source: String): Option[Long]
  def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long]
  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit
  def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit
  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit
  def removeForView(spark: SparkSession, viewName: String): Unit
}

private[common] object CdfWatermarkBackend {
  def forSession(spark: SparkSession): CdfWatermarkBackend =
    if (FeatureGate.deltaCatalogEnabled(spark)) GroupCommitDeltaCdfWatermarkBackend
    else RocksDbCdfWatermarkBackend
}

/** Delta CDF watermarks whose concurrent `putAll` calls in this JVM share one write per group commit.
  *
  * When every row in a batch is already stored, the write is one UPDATE that names only `view_name` partitions, so
  * Delta rewrites them in one pass. Otherwise it is one MERGE that also inserts the new rows. A MERGE with inserts
  * joins source and target in a full outer join, which adaptive execution plans as one task that writes every touched
  * partition in turn.
  */
private[common] object GroupCommitDeltaCdfWatermarkBackend extends CdfWatermarkBackend {
  private val Delegate = DeltaCdfWatermarkBackend
  private val ViewName = "view_name"
  private val Source   = "source"
  private val Version  = "version"

  private val schema = StructType(
    Seq(
      StructField(ViewName, StringType, nullable = false),
      StructField(Source, StringType, nullable = false),
      StructField(Version, LongType, nullable = false)
    )
  )

  private final case class Put(viewName: String, versionsBySource: Map[String, Long])
  private final case class Watermark(viewName: String, source: String, version: Long)

  private def path(spark: SparkSession): String = FeatureGate.catalogPath(spark).stripSuffix("/") + "/cdf_watermarks"

  override def ensureTables(spark: SparkSession): Unit = Delegate.ensureTables(spark)

  override def get(spark: SparkSession, viewName: String, source: String): Option[Long] =
    Delegate.get(spark, viewName, source)

  override def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long] =
    Delegate.getAll(spark, viewName, sources)

  override def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    putAll(spark, viewName, Map(source -> version))

  override def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit =
    if (versionsBySource.nonEmpty)
      DeltaGroupCommit
        .forPath[Put](path(spark))
        .submit(Put(viewName, versionsBySource))(p => putBatch(spark, Seq(p)))(putBatch(spark, _))

  /** Applies the puts with the same per-row semantics as sequential `putAll`s: a watermark only moves forward. */
  private def putBatch(spark: SparkSession, puts: Seq[Put]): Unit =
    OpenIvmMetrics.time("catalog.cdf_watermark.put_all_batch") {
      val watermarks = puts
        .flatMap(p => p.versionsBySource.toSeq.map { case (source, version) => (p.viewName, source) -> version })
        .groupBy(_._1)
        .toSeq
        .map { case ((view, source), versions) => Watermark(view, source, versions.map(_._2).max) }
      val views = watermarks.map(_.viewName).distinct
      Delegate.withDeltaRetry {
        Delegate.ensureTables(spark)
        val stored = spark.read
          .format("delta")
          .load(path(spark))
          .filter(col(ViewName).isin(views: _*))
          .select(ViewName, Source)
          .collect()
          .map(row => row.getString(0) -> row.getString(1))
          .toSet
        if (watermarks.forall(w => stored.contains(w.viewName -> w.source))) update(spark, views, watermarks)
        else merge(spark, views, watermarks)
      }
    }

  private def update(spark: SparkSession, views: Seq[String], watermarks: Seq[Watermark]): Unit = {
    def forward(w: Watermark): (Column, Column) =
      (col(ViewName) === lit(w.viewName) && col(Source) === lit(w.source), greatest(col(Version), lit(w.version)))
    val (firstKey, firstVersion) = forward(watermarks.head)
    val version = watermarks.tail
      .foldLeft(when(firstKey, firstVersion)) { (acc, w) =>
        val (key, forwarded) = forward(w)
        acc.when(key, forwarded)
      }
      .otherwise(col(Version))
    DeltaTable.forPath(spark, path(spark)).update(col(ViewName).isin(views: _*), Map(Version -> version))
  }

  private def merge(spark: SparkSession, views: Seq[String], watermarks: Seq[Watermark]): Unit = {
    val rows     = watermarks.map(w => Row(w.viewName, w.source, w.version))
    val incoming = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
    DeltaTable
      .forPath(spark, path(spark))
      .as("target")
      .merge(
        incoming.as("incoming"),
        col(s"target.$ViewName").isin(views: _*) &&
          col(s"target.$ViewName") === col(s"incoming.$ViewName") &&
          col(s"target.$Source") === col(s"incoming.$Source")
      )
      .whenMatched(s"incoming.$Version > target.$Version")
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit =
    Delegate.removeForBaseTable(spark, baseTable)

  override def removeForView(spark: SparkSession, viewName: String): Unit = Delegate.removeForView(spark, viewName)
}
