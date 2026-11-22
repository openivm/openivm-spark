package org.openivm.spark.common

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

private[common] object DeltaPathTable {
  def createIfAbsent(spark: SparkSession, path: String, schema: StructType, partitionColumn: String): Unit =
    spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .write
      .format("delta")
      .mode("ignore")
      .partitionBy(partitionColumn)
      .save(path)
}
