package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CombiningSerializations(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(col("customer"), explode(col("events")).alias("events"))
      .groupBy(col("customer"))
      .agg(collect_list(col("events")).alias("events"))
  }
}


object CombiningSerializations {

  def apply(config: Config, spark: SparkSession) = {
    new CombiningSerializations(config, spark)
  }
}