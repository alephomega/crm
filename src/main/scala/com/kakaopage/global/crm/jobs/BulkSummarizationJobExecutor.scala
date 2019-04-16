package com.kakaopage.global.crm.jobs

import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.global.crm.BulkGlueJob
import com.kakaopage.global.crm.transformations.BulkSummarization
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class BulkSummarizationJobExecutor(config: Config, glueContext: GlueContext) extends BulkGlueJob(config, glueContext) {
  val summarization = BulkSummarization(config, sparkSession)

  override def transform(dynamicFrames: DynamicFrame*): DynamicFrame = {
    toDynamicFrame(summarization.transform(dynamicFrames.map(_.toDF()): _*))
  }
}

object BulkSummarizationJobExecutor {

  def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
    args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
    new BulkSummarizationJobExecutor(ConfigFactory.load("bulk-summarization"), glueContext)
  }

  def main(sysArgs: Array[String]) = {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val options = Seq("JOB_NAME", "year", "month")
    val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    BulkSummarizationJobExecutor(args, options, glueContext).run()
    Job.commit()
  }
}