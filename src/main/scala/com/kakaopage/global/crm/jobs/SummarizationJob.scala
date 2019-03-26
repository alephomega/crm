package com.kakaopage.global.crm.jobs

import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.global.crm.GlueJob
import com.kakaopage.global.crm.transformations.Summarization
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class SummarizationJob(config: Config, glueContext: GlueContext) extends GlueJob(config, glueContext) {
  val summarization = Summarization(config, sparkSession)

  override def transform(dynamicFrame: DynamicFrame): DynamicFrame = {
    toDynamicFrame(summarization.transform(dynamicFrame.toDF()))
  }
}

object SummarizationJob {

  def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
    args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
    new SummarizationJob(ConfigFactory.load("summarization"), glueContext)
  }

  def main(sysArgs: Array[String]) = {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val options = Seq("JOB_NAME", "date", "hour")
    val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    SummarizationJob(args, options, glueContext).run()
    Job.commit()
  }
}