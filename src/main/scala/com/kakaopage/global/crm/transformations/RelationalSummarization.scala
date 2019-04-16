package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{SparkSession, _}

class RelationalSummarization(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val events = dataFrames(0)
    val series = dataFrames(1)

    val agg = events.filter(not(col("event") === "purchase") || col("meta.item") === "ticket")
      .groupBy(col("customer"), col("event"), col("meta.series").as("series"))
      .agg(count("*").as("frequency"))

    val meta = series.withColumnRenamed("id", "series").withColumn("categories", from_json(col("category"), new ArrayType(StringType, true))).drop(col("category"))

    agg.join(meta, Seq("series")).withColumn("version", lit(config.getString("version"))).repartition(config.getInt("sink.partitions"))
  }
}

object RelationalSummarization {
  def apply(config: Config, spark: SparkSession) = {
    new RelationalSummarization(config, spark)
  }
}