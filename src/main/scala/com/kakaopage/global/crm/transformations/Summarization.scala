package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.{Aggregation, Transformation}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class Summarization(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

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
}

object Summarization {
  def apply(config: Config, spark: SparkSession) = {
    new Summarization(config, spark)
  }
}