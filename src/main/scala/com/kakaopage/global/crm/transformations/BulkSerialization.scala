package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._

class BulkSerialization(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    dataFrames(0).groupBy("date", "hour", "customer").agg(collect_list(struct("id", "at", "event", "meta")).alias("events"))
  }
}

object BulkSerialization {
  def apply(config: Config, spark: SparkSession) = {
    new BulkSerialization(config, spark)
  }
}
