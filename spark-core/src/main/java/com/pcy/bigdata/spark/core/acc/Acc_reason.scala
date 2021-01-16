package com.pcy.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Acc_reason {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // reduce : 分区内计算，分区间计算
    //val i: Int = rdd.reduce(_+_)
    //println(i)
    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println("sum = " + sum)
    // 最终结果为0，因为在Executor端计算的sum不会返回到Driver端，在Driver端打印的sum值一直还是0
    sc.stop()

  }
}
