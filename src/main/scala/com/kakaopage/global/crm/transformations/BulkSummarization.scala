package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.{Summarizer, Transformation}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

class BulkSummarization(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    dataFrames(0)
      .filter(not(col("at").isNull))
      .groupBy(col("date"), col("hour"), col("customer"), col("event"))
      .agg(Summarizer(config.getString("service.timezone")).toColumn.alias("summary"))
      .select(col("customer"), col("name"), col("summary.last").alias("last"), col("summary.frequency").alias("frequency"), col("summary.distribution").alias("distribution"))
  }
}

object BulkSummarization {
  def apply(config: Config, spark: SparkSession) = {
    new BulkSummarization(config, spark)
  }
}