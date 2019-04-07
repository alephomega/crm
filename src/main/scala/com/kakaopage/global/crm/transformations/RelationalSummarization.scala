package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

class RelationalSummarization(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val events = dataFrames(0)
    val seriesMetadata = dataFrames(1)

    val agg = events.groupBy(col("customer"), col("event"), col("meta.series").as("series")).agg(count("*").as("frequency"))
    agg.join(seriesMetadata, Seq("series"))
  }
}

object RelationalSummarization {
  def apply(config: Config, spark: SparkSession) = {
    new RelationalSummarization(config, spark)
  }
}