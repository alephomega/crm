package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.Transformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

class RelationalSummarization(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  val categories = udf((s: String) => {
    s.substring(1, s.length-1).split(",").map(_.trim.replaceAllLiterally("\"", ""))
  })

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val events = dataFrames(0)
    val series = dataFrames(1)

    val agg = events.filter(col("meta.series").isNotNull)
      .groupBy(col("customer"), col("event"), col("meta.series").as("series"))
      .agg(count("*").as("frequency"))

    val meta = series.withColumnRenamed("id", "series").withColumn("categories", categories(col("category"))).drop(col("category"))

    agg.join(meta, Seq("series")).repartition(config.getInt("sink.partitions"))
  }
}

object RelationalSummarization {
  def apply(config: Config, spark: SparkSession) = {
    new RelationalSummarization(config, spark)
  }
}