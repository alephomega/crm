package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Serialization(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrame: DataFrame): DataFrame = {
      dataFrame.groupBy("u").agg(collect_list(struct("at", "ev", "meta")).alias("h"))
  }
}

object Serialization {

  def apply(args: Map[String, String], spark: SparkSession) = {
    args.foreach(kv => sys.props.put(kv._1, kv._2))
    new Serialization(ConfigFactory.load(), spark)
  }
}