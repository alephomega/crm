package com.kakaopage.global.crm.jobs

import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.global.crm.GlueJob
import com.kakaopage.global.crm.transformations.MergingSummarizations
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class MergingSummarizationsJob(config: Config, glueContext: GlueContext) extends GlueJob(config, glueContext) {
  val merging = MergingSummarizations(config, sparkSession)

  override def transform(dynamicFrame: DynamicFrame): DynamicFrame = {
    toDynamicFrame(merging.transform(dynamicFrame.toDF()))
  }
}

object MergingSummarizationsJob {

  def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
    args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
    new MergingSummarizationsJob(ConfigFactory.load("merging-summarizations"), glueContext)
  }

  def main(sysArgs: Array[String]) = {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val options = Seq("JOB_NAME", "year", "month", "day")
    val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    MergingSummarizationsJob(args, options, glueContext).run()
    Job.commit()
  }
}