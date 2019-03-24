package com.kakaopage.global.crm.jobs

import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.global.crm.GlueJob
import com.kakaopage.global.crm.transformations.SerializationMerge
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class SerializationMergeJob(config: Config, glueContext: GlueContext) extends GlueJob(config, glueContext) {
  val merge = SerializationMerge(config, sparkSession)

  override def transform(dynamicFrame: DynamicFrame): DynamicFrame = {
    toDynamicFrame(merge.transform(dynamicFrame.toDF()))
  }
}

object SerializationMergeJob {

  def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
    args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
    new SerializationMergeJob(ConfigFactory.load("serialization-merge"), glueContext)
  }

  def main(sysArgs: Array[String]): Unit = {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val options = Seq("JOB_NAME", "year", "month", "day")
    val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    SerializationMergeJob(args, options, glueContext).run()
    Job.commit()
  }
}

