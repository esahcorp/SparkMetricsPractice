package com.kuma.spark.metrics.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingMetrics, StreamingMetricsListener}
import org.apache.spark.{SparkConf, SparkEnv}

/**
 *
 *
 * @author lostkite@outlook.com 2020-02-21
 */
object StreamingWordCount extends Logging {

  //  启动程序时需要传入 Graphite（or Graphite Exporter） 的地址和端口
  def main(args: Array[String]): Unit = {
    consume(args(0), args(1))
  }

  def consume(graphiteHost: String, graphitePort: String): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
      .set("spark.metrics.conf.*.sink.graphite.host", graphiteHost)
      .set("spark.metrics.conf.*.sink.graphite.port", graphitePort)
      .set("spark.metrics.conf.*.sink.graphite.period", "1")
      .set("spark.metrics.conf.*.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
      .setAppName("StreamingWordCounter")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // 收集信息
    val metricsListener = new StreamingMetricsListener(ssc)
    ssc.addStreamingListener(metricsListener)
    // 将信息注册为 Metrics，提供导出功能
    val metrics = new StreamingMetrics(metricsListener)
    SparkEnv.get.metricsSystem.registerSource(metrics)

    val result = ssc.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    result.print
    println(result)

    ssc.start()
    ssc.awaitTermination()
  }

}
