package com.kuma.spark.metrics.structured

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{StreamingQueryListener, StructuredMetrics, StructuredMetricsListener, Trigger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkEnv}

/**
 *
 *
 * @author lostkite@outlook.com 2020-02-25
 */
object StructuredWC {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StructuredNetworkWordCount")
      .set("spark.sql.streaming.metricsEnabled", "true")
    //      .set("spark.metrics.conf.*.source.my.class", "org.apache.spark.metrics.source.MyMetrics")
    val spark = SparkSession
      .builder
      .config(conf)
      //      .master("local[2]")
      //      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    //    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    //    spark.conf.set("spark.metrics.conf.*.source.my.class", "org.apache.spark.metrics.source.MyMetrics")
    // 注册 Metrics
    val metricsListener = new StructuredMetricsListener()
    spark.streams.addListener(metricsListener)
    val metrics = new StructuredMetrics(metricsListener)
    SparkEnv.get.metricsSystem.registerSource(metrics)

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String](Encoders.STRING).flatMap(_.split(" "))(Encoders.STRING)

    // Generate running word count
    val wordCounts = words.groupBy("value").count()



    // trigger interval

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()

//    spark.listenerManager.register(new QueryExecutionListener {
//      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = ???
//
//      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ???
//    })
    var i = 0
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        if (i < 5) {
          i += 1
        } else {
          query.stop()
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })
    query.awaitTermination()
    // Application 运行结束移除 Metrics
    SparkEnv.get.metricsSystem.removeSource(metrics)

    TimeUnit.MINUTES.sleep(5);
  }

}
