package org.apache.spark.sql.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.AppState
import org.apache.spark.AppState.AppState
import org.apache.spark.internal.Logging

/**
 * todo listener 自动注册：参考 Resource 配置文件注册的过程
 *
 * @author lostkite@outlook.com 2020-02-26
 */
class StructuredMetricsListener extends StreamingQueryListener with Logging {

  private var _lastReceivedBatchRecords = 0L
  private var _applicationState: AppState = AppState.WAITING
  private var _applicationFailedReason: Option[String] = None
  private var _totalReceivedRecords: Option[Long] = None
  private var _totalMemoryUsedBytes: Option[Long] = None

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logInfo(s"Start monitor structured streaming query(id: ${event.id}, name: ${event.name})")
    applicationRunning()
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress: StreamingQueryProgress = event.progress
    _lastReceivedBatchRecords = progress.numInputRows
    val state: Option[StateOperatorProgress] = progress.stateOperators.headOption
    _totalReceivedRecords = state.map(_.numRowsTotal)
    _totalMemoryUsedBytes = state.map(_.memoryUsedBytes)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logInfo(s"Streaming Query(${event.id}) terminated, waiting for finished process...")
    _applicationFailedReason = event.exception
    event.exception match {
      case None => _applicationState = AppState.FINISHED
      case Some(_) =>
        _applicationState = AppState.FAILED
        // 线程挂起，等待指标收集时间
        TimeUnit.SECONDS.sleep(10)
    }
  }

  def applicationState: Int = _applicationState.id

  def applicationFailedReason: Option[String] = _applicationFailedReason

  def lastReceivedBatchRecords: Long = _lastReceivedBatchRecords

  def totalReceivedRecords: Option[Long] = _totalReceivedRecords

  def totalProcessedRecords: Option[Long] = _totalReceivedRecords.map(_.longValue() - _lastReceivedBatchRecords)

  def totalMemoryUsedBytes: Option[Long] = _totalMemoryUsedBytes

  private def applicationRunning(): Unit = {
    if (_applicationState != AppState.RUNNING)
      _applicationState = AppState.RUNNING
  }

}

