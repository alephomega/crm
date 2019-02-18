package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.{Aggregation, Transformation}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class Summarization(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  val from: String = config.getString("source.from")
  val duration: Int = config.getInt("source.duration")

  val schema: StructType = DataType.fromJson(config.getString("sink.schema")).asInstanceOf[StructType]

  case class TimeLog(at: String, event: String)

  def toTimeLog(r: Row): TimeLog = TimeLog(r.getAs[String]("at"), r.getAs[String]("ev"))


  override def transform(dataFrame: DataFrame): DataFrame = {
    val rdd: RDD[Row] = dataFrame.rdd
      .map(r => (r.getAs[String]("u"), summarize(r.getAs[Seq[Row]]("h").map(toTimeLog))))
      .flatMap({
        case (customer: String, summaries: Map[String, Aggregation]) =>
          summaries.map {
            case (event: String, aggregation: Aggregation) =>  Row(customer, from, duration, event, aggregation.last, aggregation.frequency, aggregation.distribution.toRow())
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

  def apply(args: Map[String, String], spark: SparkSession) = {
    args.foreach(kv => sys.props.put(kv._1, kv._2))
    new Summarization(ConfigFactory.load(), spark)
  }
}