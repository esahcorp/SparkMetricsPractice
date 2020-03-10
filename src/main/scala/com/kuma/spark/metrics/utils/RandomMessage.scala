package com.kuma.spark.metrics.utils

/**
 *
 *
 * @author lostkite@outlook.com 2020-03-06
 */
object RandomMessage {

  def main(args: Array[String]): Unit = {
    println(RandomMessage.floatSizeMessage())
  }

  def floatSizeMessage(maxSize: Int = 10): String = {
    import scala.util.Random
    val wordPerMessage = Random.nextInt(maxSize)
    val str: Seq[String] = (1 to wordPerMessage).map(_ => BigInt(72, Random).toString(36))
    str mkString " "
  }
}
