package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class FirstPurchases(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val firstPurchases = dataFrames(0).select(col("customer"), col("at"))
    val purchases = dataFrames(1)
      .filter(col("event").equalTo("purchase") && col("meta.item").equalTo("cash") && col("meta.paid_amount").gt(0))
      .select(col("customer"), col("at"))

    firstPurchases.union(purchases).groupBy("customer").agg(min("at").as("at")).repartition(config.getInt("sink.partitions"))
  }
}

object FirstPurchases {

  def apply(config: Config, spark: SparkSession) = {
    new FirstPurchases(config, spark)
  }
}
