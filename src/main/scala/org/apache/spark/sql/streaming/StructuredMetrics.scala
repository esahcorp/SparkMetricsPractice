package org.apache.spark.sql.streaming

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.{Source => CodahaleSource}

/**
 *
 *
 * @author lostkite@outlook.com 2020-02-25
 */
class StructuredMetrics(metricsListener: StructuredMetricsListener) extends CodahaleSource with Logging {

  override val sourceName: String = "StreamingMetrics"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  registerGauge("applicationStatus", _.applicationState, 5)
  registerGauge("lastReceivedBatch_records", _.lastReceivedBatchRecords, 0L)

  registerGaugeWithOption("totalReceivedRecords", _.totalReceivedRecords, 0L)
  registerGaugeWithOption("totalMemoryUsedBytes", _.totalMemoryUsedBytes, 0L)
  metricRegistry.counter(MetricRegistry.name("streaming.applicationFailReason.%s").format(metricsListener.applicationFailedReason))

  private def registerGauge[T](name: String,
                               f: StructuredMetricsListener => T,
                               defaultValue: T): Unit = {
    registerGaugeWithOption[T](name,
      (_: StructuredMetricsListener) => Option(f(metricsListener)), defaultValue)
  }

  private def registerGaugeWithOption[T](name: String,
                                         f: StructuredMetricsListener => Option[T],
                                         defaultValue: T): Unit = {
    metricRegistry.register(MetricRegistry.name("streaming", name), new Gauge[T] {
      override def getValue: T = f(metricsListener).getOrElse(defaultValue)
    })
  }
}