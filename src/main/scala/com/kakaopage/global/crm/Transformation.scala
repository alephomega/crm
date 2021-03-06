package com.kakaopage.global.crm

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Transformation(val config: Config, spark: SparkSession) extends Serializable {
  def transform(dataFrames: DataFrame*): DataFrame
}