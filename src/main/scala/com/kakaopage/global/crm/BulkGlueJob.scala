package com.kakaopage.global.crm

import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DataSink, DataSource, DynamicFrame, GlueContext}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._


abstract class BulkGlueJob(val config: Config, glueContext: GlueContext) {
  val sparkSession: SparkSession = glueContext.getSparkSession

  def run() = sink.writeDynamicFrame(transform(sources.map(_.getDynamicFrame()): _*))

  def sources: Seq[DataSource] = {
    config.getConfigList("sources").asScala.map(source =>
      glueContext.getCatalogSource(
        database = source.getString("database"),
        tableName = source.getString("table"),
        transformationContext = source.getString("context"),
        pushDownPredicate = source.getString("pushdown-predicate"))
    )
  }

  def sink: DataSink = {
    val path = config.getString("sink.path")
    val partitionKeys = config.getStringList("sink.partition-keys").asScala

    glueContext.getSinkWithFormat(
      connectionType = "s3",
      options = JsonOptions(Map("path" -> s"s3://$path", "partitionKeys" -> partitionKeys, "groupFiles" -> "inPartition")),
      transformationContext = config.getString("sink.context"),
      format = config.getString("sink.format"))
  }

  def toDynamicFrame(df: DataFrame): DynamicFrame = {
    DynamicFrame(df, glueContext)
  }

  def toDynamicFrame(rdd: RDD[Row], schema: StructType): DynamicFrame = {
    toDynamicFrame(glueContext.createDataFrame(rdd, schema))
  }

  def transform(dynamicFrames: DynamicFrame*): DynamicFrame

  def repartition(dynamicFrame: DynamicFrame, partitions: Int): DynamicFrame = {
    dynamicFrame.repartition(
      numPartitions = partitions
    )
  }
}

