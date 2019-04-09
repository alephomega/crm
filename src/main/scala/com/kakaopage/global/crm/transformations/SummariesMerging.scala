package com.kakaopage.global.crm.transformations

import com.kakaopage.global.crm.{Aggregation, Distribution, Summary, Transformation}
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SummariesMerging(config: Config, spark: SparkSession) extends Transformation(config, spark) with Serializable {

  override def transform(dataFrames: DataFrame*): DataFrame = {
    val rdd: RDD[Row] = dataFrames(0).rdd
    rdd.map(toSummary)
      .groupBy(s => (s.customer, s.event))
      .map {
        case ((customer: String, event: String), summaries: Iterable[Summary]) => {
          val aggregation = merge(summaries)
          Row.merge(Seq[Row](Row(customer, event), aggregation.toRow): _*)
        }
      }

      spark.createDataFrame(rdd, schema)
  }

  def merge(summaries: Iterable[Summary]): Aggregation = {
    summaries.foldLeft(Aggregation(config.getString("service.timezone"))) ((aggregation, summary) => Aggregation.aggregate(aggregation, summary))
  }

  def toSummary(row: Row): Summary = {
    val distribution = row.getAs[Row]("distribution")
    Summary(row.getAs[String]("customer"), row.getAs[String]("event"), row.getAs[String]("last"), row.getAs[Int]("frequency"), Distribution(distribution.getAs[Array[Int]]("hour"), distribution.getAs[Array[Int]]("day")))
  }

  val schema: StructType = DataType.fromJson(config.getString("sink.schema")).asInstanceOf[StructType]
}


object SummariesMerging {

  def apply(config: Config, spark: SparkSession) = {
    new SummariesMerging(config, spark)
  }
}