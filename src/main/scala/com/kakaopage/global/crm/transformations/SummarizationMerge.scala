package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.{Aggregation, Distribution, Summary, Transformation}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SummarizationMerge(config: Config, spark: SparkSession) extends Transformation(config, spark) {

  override def transform(dataFrame: DataFrame): DataFrame = {
    val rdd: RDD[Row] = dataFrame.rdd
    rdd.map(toSummary)
      .groupBy(s => (s.customer, s.event))
      .map {
        case ((customer: String, event: String), summaries: Iterable[Summary]) => {
          val aggregation = merge(summaries)
          Row(customer, from, duration, event, Aggregation.format(aggregation.last.get), aggregation.frequency, aggregation.distribution.toRow())
        }
      }

      spark.createDataFrame(rdd, schema)
  }

  def merge(summaries: Iterable[Summary]): Aggregation = {
    summaries.foldLeft(Aggregation(config.getString("service.timezone"))) ((aggregation, summary) => Aggregation.aggregate(aggregation, summary))
  }

  def toSummary(row: Row): Summary = {
    val distribution = row.getAs[Row]("distribution")
    Summary(row.getAs[String]("u"), from, duration, row.getAs[String]("ev"), row.getAs[String]("last"), row.getAs[Int]("frequency"), Distribution(distribution.getAs[Array[Int]]("hour"), distribution.getAs[Array[Int]]("day")))
  }

  val from: String = config.getString("source.from")
  val duration: Int = config.getInt("source.duration")
  val schema: StructType = DataType.fromJson(config.getString("sink.schema")).asInstanceOf[StructType]
}


object SummarizationMerge {

  def apply(args: Map[String, String], spark: SparkSession) = {
    args.foreach(kv => sys.props.put(kv._1, kv._2))
    new SummarizationMerge(ConfigFactory.load(), spark)
  }
}