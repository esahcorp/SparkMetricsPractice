package com.kuma.spark.metrics

import java.io.{IOException, OutputStreamWriter, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.TimeUnit

import scala.util.control.Breaks.{break, breakable}

import com.kuma.spark.metrics.utils.RandomMessage

/**
 * Socket Server to send messages continuously, in order to simulate an streaming data source.
 *
 * @author lostkite@outlook.com 2020-02-21
 */
object Producer {

  def main(args: Array[String]): Unit = {
    var serverSocket: ServerSocket = null
    var pw: PrintWriter = null
    try {
      serverSocket = new ServerSocket(9999)
      serverSocket.setSoTimeout(10000)
      println("P：服务启动，等待连接")

      var socket: Socket = serverSocket.accept()
      println("P：连接成功：" + socket.getRemoteSocketAddress)
      pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream))
      var times = 0
      breakable {
        while (true) {
          if (!(socket.isBound && socket.isConnected)) {
            println("连接异常，停止输出 ...")
            break
          }
          val str = "P: spark streaming test " + times + RandomMessage.floatSizeMessage()
          pw.println(str)
          pw.flush()
          System.out.println(str)
          try TimeUnit.MILLISECONDS.sleep(100)
          catch {
            case e: InterruptedException =>
              Thread.currentThread().interrupt()
              e.printStackTrace()
          }
          times += 1
        }
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      try {
        pw.close()
        serverSocket.close()
      } catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
  }

}
