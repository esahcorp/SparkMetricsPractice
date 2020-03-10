package org.apache.spark

/**
 * Application 状态，maybe not all useful
 *
 * @author lostkite@outlook.com 2020-03-05
 */
object AppState extends Enumeration {

  type AppState = Value

  // 从 0 开始
  val WAITING, RUNNING, FAILED, FINISHED, KILLED, UNKNOWN = Value
}
