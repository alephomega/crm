package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.{Summarizer, Transformation}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


class Summarization(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    /*
    dataFrames(0)
      .select(
        col("customer"),
        explode(col("events")).alias("event"))
      .select(
        col("customer"),
        col("event.event").alias("event"),
        col("event.at").alias("at"))
      .groupBy(
        col("customer"),
        col("event"))
      .agg(Summarizer(config.getString("service.timezone")).toColumn)
      .repartition(config.getInt("sink.partitions"))
      */

    dataFrames(0)
      .select(col("customer"), col("event"), col("at"))
      .groupBy(
        col("customer"),
        col("event"))
      .agg(Summarizer(config.getString("service.timezone")).toColumn.alias("summary"))
      .select(col("customer"), col("event"), col("summary.last").alias("last"), col("summary.frequency").alias("frequency"), col("summary.distribution").alias("distribution"))
      .repartition(config.getInt("sink.partitions"))
  }

  /*
  val schema: StructType = DataType.fromJson(config.getString("sink.schema")).asInstanceOf[StructType]

  case class TimeLog(at: String, event: String)
  def toTimeLog(r: Row): TimeLog = TimeLog(r.getAs[String]("at"), r.getAs[String]("event"))

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val rdd: RDD[Row] = dataFrames(0).rdd
      .map(r => (r.getAs[String]("customer"), summarize(r.getAs[Seq[Row]]("events").map(toTimeLog))))
      .flatMap({
        case (customer: String, summaries: Map[String, Aggregation]) =>
          summaries.map {
            case (event: String, aggregation: Aggregation) =>  Row.merge(Seq[Row](Row(customer, event), aggregation.toRow): _*)
          }
      })

    spark.createDataFrame(rdd, schema)
  }

  def summarize(logs: Seq[TimeLog]): Map[String, Aggregation] = {
    logs.groupBy(_.event)
      .map {
        case (event: String, logs: Seq[TimeLog]) => {
          val result = logs.foldLeft(Aggregation(config.getString("service.timezone"))) (
            (aggregation: Aggregation, log: TimeLog) => Aggregation.aggregate(aggregation, log.at)
          )

          (event, result)
        }
      }
  }

  */
}

object Summarization {
  def apply(config: Config, spark: SparkSession) = {
    new Summarization(config, spark)
  }
}