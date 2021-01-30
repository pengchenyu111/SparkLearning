package com.pcy.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Transform 允许 DStream 上执行任意的 RDD-to-RDD 函数。
 * 即使这些函数并没有在 DStream的 API 中暴露出来，通过该函数可以方便的扩展 Spark API。
 * 该函数每一批次调度一次。其实也就是对 DStream 中的 RDD 应用转换
 */
object SparkStreaming_State_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)

    // transform方法可以将底层RDD获取到后进行操作
    // 1. DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code : Driver端，一次
    val newDS: DStream[(String, Int)] = lines.transform(
      // Code : Driver端，（周期性执行）
      rdd => {
        // Code : Executor端
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
        val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
        value
      }
    )
    // Code : Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        // Code : Executor端
        data
      }
    )

    newDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
