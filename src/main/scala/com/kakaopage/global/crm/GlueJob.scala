package com.kakaopage.global.crm

import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DataSink, DataSource, DynamicFrame, GlueContext}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class GlueJob(val config: Config, glueContext: GlueContext) {
  val sparkSession: SparkSession = glueContext.getSparkSession

  def run() = sink.writeDynamicFrame(transform(source.getDynamicFrame()))

  def source: DataSource = {
    glueContext.getCatalogSource(
      database = config.getString("source.database"),
      tableName = config.getString("source.table"),
      transformationContext = config.getString("source.context"),
      pushDownPredicate = config.getString("source.pushdown-predicate"))
  }

  def sink: DataSink = {
    val path = config.getString("sink.path")
    glueContext.getSinkWithFormat(
      connectionType = "s3",
      options = JsonOptions(f"""{"path": "s3://$path%s"}"""),
      transformationContext = config.getString("sink.context"),
      format = config.getString("sink.format"))
  }

  def toDynamicFrame(df: DataFrame): DynamicFrame = {
    DynamicFrame(df, glueContext)
  }

  def toDynamicFrame(rdd: RDD[Row], schema: StructType): DynamicFrame = {
    toDynamicFrame(glueContext.createDataFrame(rdd, schema))
  }

  def transform(dynamicFrame: DynamicFrame): DynamicFrame

  def repartition(dynamicFrame: DynamicFrame, partitions: Int): DynamicFrame = {
    dynamicFrame.repartition(
      numPartitions = partitions
    )
  }
}
