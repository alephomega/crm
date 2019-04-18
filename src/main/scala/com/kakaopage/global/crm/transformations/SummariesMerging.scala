package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class SummariesMerging(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    dataFrames(0)
      .groupBy(col("customer"), col("event"))
      .agg(Merger(config.getString("service.timezone")).toColumn.alias("summary"))
      .select(col("customer"), col("event"), col("summary.last").alias("last"), col("summary.frequency").alias("frequency"), col("summary.distribution").alias("distribution"))
      .repartition(config.getInt("sink.partitions"))
  }
}


object SummariesMerging {

  def apply(config: Config, spark: SparkSession) = {
    new SummariesMerging(config, spark)
  }
}