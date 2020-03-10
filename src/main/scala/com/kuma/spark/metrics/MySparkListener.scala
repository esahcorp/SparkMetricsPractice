package com.kuma.spark.metrics

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}

/**
 *
 *
 * @author lostkite@outlook.com 2020-02-25
 */
class MySparkListener extends SparkListener with Logging{

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println("XXXX-start")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("XXXX-stop")

  }

}
