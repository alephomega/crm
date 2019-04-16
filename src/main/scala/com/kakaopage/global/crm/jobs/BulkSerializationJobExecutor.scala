package com.kakaopage.global.crm.jobs

import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.global.crm.BulkGlueJob
import com.kakaopage.global.crm.transformations.BulkSerialization
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class BulkSerializationJobExecutor(config: Config, glueContext: GlueContext) extends BulkGlueJob(config, glueContext) {
  val serialization = BulkSerialization(config, sparkSession)

  override def transform(dynamicFrames: DynamicFrame*): DynamicFrame = {
    toDynamicFrame(serialization.transform(dynamicFrames.map(_.toDF()): _*))
  }
}

object BulkSerializationJobExecutor {

  def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
    args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
    new BulkSerializationJobExecutor(ConfigFactory.load("bulk-serialization"), glueContext)
  }

  def main(sysArgs: Array[String]) = {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val options = Seq("JOB_NAME", "year", "month")
    val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    BulkSerializationJobExecutor(args, options, glueContext).run()
    Job.commit()
  }
}