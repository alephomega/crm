package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class HistoryCombining(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    dataFrames(0)
      .select(col("customer"), explode(col("events")).alias("events"))
      .groupBy(col("customer"))
      .agg(collect_list(col("events")).alias("events"))
      .repartition(config.getInt("sink.partitions"))
  }
}


object HistoryCombining {

  def apply(config: Config, spark: SparkSession) = {
    new HistoryCombining(config, spark)
  }
}