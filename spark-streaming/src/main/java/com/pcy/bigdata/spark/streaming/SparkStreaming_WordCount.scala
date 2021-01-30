package com.pcy.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用 netcat 工具向 9999 端口不断的发送数据，通过 SparkStreaming 读取端口数据并统计不同单词出现的次数
 * nc -lp 9999 在cmd中开启端口
 */
object SparkStreaming_WordCount {

  def main(args: Array[String]): Unit = {

    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordToCount.print()

    // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    //ssc.stop()
    // 1. 启动采集器
    ssc.start()
    // 2. 等待采集器的关闭
    ssc.awaitTermination()
  }
}
