package org.apache.spark.streaming

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.{Source => CodahaleSource}

/**
 * A sample of custom Spark Streaming Metrics Source.
 *
 * @author lostkite@outlook.com 2020-02-25
 */
class StreamingMetrics(sml: StreamingMetricsListener) extends CodahaleSource with Logging {

  override val sourceName: String = "%s.StreamingMetrics".format(sml.applicationName)

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  registerGauge("applicationStatus", _.applicationState, 5)

  // Gauge for last supplement batch records.
  registerGauge("lastCompletedBatch_records", _.lastCompletedBatchRecords.values.sum, 0L)
  registerGauge("lastCompletedBatch_processRate", _.lastCompletedBatchProcessRate, 0.0)

  private def registerGauge[T](name: String,
                               f: StreamingMetricsListener => T,
                               defaultValue: T): Unit = {
    registerGaugeWithOption[T](name,
      (_: StreamingMetricsListener) => Option(f(sml)), defaultValue)
  }

  private def registerGaugeWithOption[T](name: String,
                                         f: StreamingMetricsListener => Option[T],
                                         defaultValue: T): Unit = {
    metricRegistry.register(MetricRegistry.name("streaming", name), new Gauge[T] {
      override def getValue: T = f(sml).getOrElse(defaultValue)
    })
  }
}