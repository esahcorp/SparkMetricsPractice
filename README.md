## Spark 程序获取信息的途径

API

* SparkStatusTracker

* AppStatusStore

自定义

* SparkListener

## Structured Streaming

* StreamingQueryListener

## Spark Streaming

API

* ReceiverTracker

* StreamingListener

## Listener

Listener 作为指标收集工具，从生命周期函数中采集关键信息，暴露给外部

* 考虑信息收集时，缓存最近 N 条数据，N 从系统配置中获取 `spark.streaming.ui.retainedBatches`

* 考虑线程安全问题


## Spark Streaming 容错机制

### 数据丢失类型

* Data received and replicated

* Data received but buffered for replication

### 故障位置

* Worker

* Driver

### 流系统步骤

1. 接收数据

2. 转换数据

3. 推送数据