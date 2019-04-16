package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

class BulkRelationalSummariesMerging(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val df = dataFrames(0)
    val cols = df.columns.filter(name => !Seq("version", "hour", "frequency").contains(name)).map(col)

    df.groupBy(cols: _*).agg(sum(col("frequency")).as("frequency")).withColumn("hour", lit("*")).withColumn("version", lit(config.getString("version")))
  }
}


object BulkRelationalSummariesMerging {

  def apply(config: Config, spark: SparkSession) = {
    new BulkRelationalSummariesMerging(config, spark)
  }
}