package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

private[common] object DeltaPathTable {
  def createIfAbsent(spark: SparkSession, path: String, schema: StructType, partitionColumn: String): Unit = {
    val logPath        = new Path(new Path(path), "_delta_log")
    val exactLogExists = logPath.getFileSystem(spark.sessionState.newHadoopConf()).exists(logPath)
    if (!exactLogExists || !DeltaTable.isDeltaTable(spark, path))
      spark
        .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        .write
        .format("delta")
        .mode("ignore")
        .partitionBy(partitionColumn)
        .save(path)
  }
}
