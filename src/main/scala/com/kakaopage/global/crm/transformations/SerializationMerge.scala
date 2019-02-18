package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class SerializationMerge(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(col("u"), explode(col("h")).alias("h"))
      .groupBy(col("u"))
      .agg(collect_list(col("h")).alias("h"))
  }
}


object SerializationMerge {

  def apply(args: Map[String, String], spark: SparkSession) = {
    args.foreach(kv => sys.props.put(kv._1, kv._2))
    new SerializationMerge(ConfigFactory.load(), spark)
  }
}