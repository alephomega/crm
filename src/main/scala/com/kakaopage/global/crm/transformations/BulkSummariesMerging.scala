package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

class BulkSummariesMerging(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    dataFrames(0)
      .groupBy(col("date"), col("customer"), col("event"))
      .agg(Merger(config.getString("service.timezone")).toColumn.alias("summary"))
      .select(col("date"), col("customer"), col("event"), col("summary.last").alias("last"), col("summary.frequency").alias("frequency"), col("summary.distribution").alias("distribution"))
      .withColumn("hour", lit("*"))
      .repartition(config.getInt("sink.partitions"))
  }
}

object BulkSummariesMerging {

  def apply(config: Config, spark: SparkSession) = {
    new BulkSummariesMerging(config, spark)
  }
}