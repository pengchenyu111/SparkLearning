package com.pcy.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 两个流之间的 join 需要两个流的批次大小一致，这样才能做到同时触发计算。
 * 计算过程就是对当前批次的两个流中各自的 RDD 进行 join，与两个 RDD 的 join 效果相同。
 */
object SparkStreaming_State_Join {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val data9999 = ssc.socketTextStream("localhost", 9999)
    val data8888 = ssc.socketTextStream("localhost", 8888)

    val map9999: DStream[(String, Int)] = data9999.map((_, 9))
    val map8888: DStream[(String, Int)] = data8888.map((_, 8))

    // 所谓的DStream的Join操作，其实就是两个RDD的join
    val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)

    joinDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
