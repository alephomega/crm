    package com.kakaopage.global.crm.jobs

    import com.amazonaws.services.glue.util.{GlueArgParser, Job}
    import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
    import com.kakaopage.global.crm.GlueJob
    import com.kakaopage.global.crm.transformations.HistoryCombining
    import com.typesafe.config.{Config, ConfigFactory}
    import org.apache.spark.SparkContext

    import scala.collection.JavaConverters._

    class HistoryCombiningJobExecutor(config: Config, glueContext: GlueContext) extends GlueJob(config, glueContext) {
      val combining = HistoryCombining(config, sparkSession)

      override def transform(dynamicFrames: DynamicFrame*): DynamicFrame = {
        toDynamicFrame(combining.transform(dynamicFrames.map(_.toDF()): _*))
      }
    }

    object HistoryCombiningJobExecutor {

      def apply(args: Map[String, String], options: Seq[String], glueContext: GlueContext) = {
        args.foreach(kv => if (options.contains(kv._1)) sys.props.put(kv._1, kv._2))
        new HistoryCombiningJobExecutor(ConfigFactory.load("history-combining"), glueContext)
      }

      def main(sysArgs: Array[String]): Unit = {
        val spark: SparkContext = new SparkContext()
        val glueContext: GlueContext = new GlueContext(spark)

        val options = Seq("JOB_NAME", "date")
        val args = GlueArgParser.getResolvedOptions(sysArgs, options.toArray)

        Job.init(args("JOB_NAME"), glueContext, args.asJava)
        HistoryCombiningJobExecutor(args, options, glueContext).run()
        Job.commit()
      }
    }

