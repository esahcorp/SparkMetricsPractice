package org.apache.spark.streaming

import scala.collection.mutable

import org.apache.spark.{AppState, SparkEnv}
import org.apache.spark.AppState.AppState
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.ui.StreamingJobProgressListener

/**
 * 采集 Streaming 监控指标
 *
 * @author lostkite@outlook.com 2020-03-05
 */
class StreamingMetricsListener(ssc: StreamingContext) extends SparkListener with StreamingListener with Logging {

  private val _applicationName = ssc.sparkContext.appName
  private val progressListener: StreamingJobProgressListener = ssc.progressListener
  private val errorReceiverInfos = new mutable.HashMap[Int, ReceiverInfo]
  private var _applicationState: AppState = AppState.WAITING
  private var _applicationFailedReason: Option[String] = None
  //  private var _applicationFailedTimes:

  // TODO: Receiver 和 streamId 的关系

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    logInfo(s"Start monitor Spark Streaming application($applicationName)")
    applicationRunning()
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    synchronized {
      _applicationState = AppState.FAILED
      errorReceiverInfos(receiverError.receiverInfo.streamId) = receiverError.receiverInfo
    }
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    val info = receiverStopped.receiverInfo
    if (!info.active) {
      _applicationState = AppState.FAILED
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = super.onBatchSubmitted(batchSubmitted)

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = super.onBatchStarted(batchStarted)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = super.onBatchCompleted(batchCompleted)

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = super.onOutputOperationStarted(outputOperationStarted)

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = super.onOutputOperationCompleted(outputOperationCompleted)


  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = synchronized {
    logInfo(s"Spark Streaming Application(${_applicationName}) terminated, waiting for finished process...")
    _applicationState = AppState.FINISHED
    SparkEnv.get.metricsSystem.report()
  }

  def applicationState: Int = _applicationState.id

  def applicationName: String = _applicationName

  //  def lastReceiveErrorMessage: Option[String] =

  private def applicationRunning(): Unit = {
    if (_applicationState != AppState.RUNNING)
      _applicationState = AppState.RUNNING
  }


  def lastCompletedBatchRecords: Map[Int, Long] = synchronized {
    val lastCompletedBlockInfoOption =
      progressListener.lastCompletedBatch.map(_.streamIdToInputInfo.mapValues(_.numRecords))
    lastCompletedBlockInfoOption.map { lastCompletedBlockInfo =>
      progressListener.streamIds.map { streamId =>
        (streamId, lastCompletedBlockInfo.getOrElse(streamId, 0L))
      }.toMap
    }.getOrElse {
      progressListener.streamIds.map(streamId => (streamId, 0L)).toMap
    }
  }

  def lastCompletedBatchProcessRate: Double = {
    if (progressListener.lastCompletedBatch.isEmpty) {
      0.0
    } else {
      ((lastCompletedBatchRecords.values.sum * 1.0) / progressListener.lastCompletedBatch.flatMap(_.totalDelay).get) * 1000
    }
  }
}
