package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

private[common] object DeltaPathTable {
  def createIfAbsent(spark: SparkSession, path: String, schema: StructType, partitionColumn: String): Unit =
    if (!DeltaTable.isDeltaTable(spark, path))
      spark
        .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        .write
        .format("delta")
        .mode("ignore")
        .partitionBy(partitionColumn)
        .save(path)
}
