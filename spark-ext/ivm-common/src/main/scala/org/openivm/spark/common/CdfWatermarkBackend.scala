package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
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
    if (FeatureGate.deltaCatalogEnabled(spark)) RecordAwareCdfWatermarkBackend
    else RocksDbCdfWatermarkBackend

  /** The Delta watermark table, read together with consumed versions whose refresh record is committed to the MV
    * log but whose watermark write is still queued. */
  private object RecordAwareCdfWatermarkBackend extends CdfWatermarkBackend {
    override def ensureTables(spark: SparkSession): Unit = DeltaCdfWatermarkBackend.ensureTables(spark)

    override def get(spark: SparkSession, viewName: String, source: String): Option[Long] =
      getAll(spark, viewName, Seq(source)).get(source)

    override def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long] = {
      val queued = MvRefreshRecords.pendingConsumed(spark, viewName, sources)
      MvRefreshRecords.maxMerge(DeltaCdfWatermarkBackend.getAll(spark, viewName, sources), queued)
    }

    override def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
      DeltaCdfWatermarkBackend.put(spark, viewName, source, version)

    override def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit =
      DeltaCdfWatermarkBackend.putAll(spark, viewName, versionsBySource)

    override def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = {
      MvRefreshRecords.forgetSource(spark, baseTable)
      DeltaCdfWatermarkBackend.removeForBaseTable(spark, baseTable)
    }

    override def removeForView(spark: SparkSession, viewName: String): Unit = {
      MvRefreshRecords.forgetView(spark, viewName)
      DeltaCdfWatermarkBackend.removeForView(spark, viewName)
    }
  }

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

  /** Raises the Delta watermarks of several views under `catalogPath` in one commit. */
  def putAllViews(spark: SparkSession, catalogPath: String, versionsByView: Map[String, Map[String, Long]]): Unit = {
    val rows = versionsByView.toSeq.flatMap { case (viewName, versions) =>
      versions.toSeq.map { case (source, version) => Row(viewName, source, version) }
    }
    if (rows.nonEmpty) OpenIvmMetrics.time("catalog.cdf_watermark.put_all") {
      val path = catalogPath.stripSuffix("/") + "/cdf_watermarks"
      DeltaCdfWatermarkBackend.withDeltaRetry {
        DeltaPathTable.createIfAbsent(spark, path, schema, ViewName)
        val incoming = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
        DeltaTable
          .forPath(spark, path)
          .as("target")
          .merge(
            incoming.as("incoming"),
            col(s"target.$ViewName").isin(versionsByView.keys.toSeq: _*) &&
              col(s"target.$ViewName") === col(s"incoming.$ViewName") &&
              col(s"target.$Source") === col(s"incoming.$Source")
          )
          .whenMatched(s"incoming.$Version > target.$Version")
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
      }
    }
  }
}
