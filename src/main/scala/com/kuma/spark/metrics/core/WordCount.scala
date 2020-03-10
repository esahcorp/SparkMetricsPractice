package com.kuma.spark.metrics.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author lostkite@outlook.com 2019-11-05
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCounter")
    conf.setMaster("local[*]")
    conf.set("spark.metrics.conf.*.source.my.class", "org.apache.spark.metrics.source.MyMetrics")
    // 避免 onApplicationStart 调用不到
    conf.set("spark.extraListeners", "com.kuma.MySparkListener")
    val sc = new SparkContext(conf)
    val result = sc.textFile("input-source", 1)
      .flatMap(_.split(" "))
      .map((_, 1))
      .localCheckpoint()
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    result.foreach(println)
  }
}
