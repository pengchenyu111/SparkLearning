package com.pcy.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * DStream 的创建
 * 通过使用 ssc.queueStream(queueOfRDDs)来创建 DStream，每一个推送到这个队列中的 RDD，都会作为一个 DStream 处理
 */
object SparkStreaming_Queue {

  def main(args: Array[String]): Unit = {

    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
