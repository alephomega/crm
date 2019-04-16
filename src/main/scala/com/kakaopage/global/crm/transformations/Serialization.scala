package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Serialization(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrames: DataFrame*): DataFrame = {
      dataFrames(0).groupBy("customer").agg(collect_list(struct("id", "at", "event", "meta")).alias("events")).repartition(config.getInt("sink.partitions"))
  }
}

object Serialization {
  def apply(config: Config, spark: SparkSession) = {
    new Serialization(config, spark)
  }
}