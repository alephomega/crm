package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

class BulkHistoryCombining(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    dataFrames(0)
      .select(col("date"), col("customer"), explode(col("events")).alias("events"))
      .groupBy(col("date"), col("customer"))
      .agg(collect_list(col("events")).alias("events"))
      .withColumn("hour", lit("*"))
  }
}


object BulkHistoryCombining {

  def apply(config: Config, spark: SparkSession) = {
    new BulkHistoryCombining(config, spark)
  }
}