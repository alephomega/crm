package com.kakaopage.global.crm.jobs

import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.global.crm.BulkGlueJob
import com.kakaopage.global.crm.transformations.BulkHistoryCombining
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class BulkHistoryCombiningJobExecutor(config: Config, glueContext: GlueContext) extends BulkGlueJob(config, glueContext) {
  val combining = BulkHistoryCombining(config, sparkSession)

  override def transform(dynamicFrames: DynamicFrame*): DynamicFrame = {
    toDynamicFrame(combining.transform(dynamicFrames.map(_.toDF()): _*))
  }
}

object BulkHistoryCombiningJobExecutor {

  def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
    args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
    new BulkHistoryCombiningJobExecutor(ConfigFactory.load("bulk-history-combining"), glueContext)
  }

  def main(sysArgs: Array[String]): Unit = {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val options = Seq("JOB_NAME", "year", "month")
    val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    BulkHistoryCombiningJobExecutor(args, options, glueContext).run()
    Job.commit()
  }
}

