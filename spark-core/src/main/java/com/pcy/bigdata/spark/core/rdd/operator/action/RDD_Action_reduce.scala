package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduce 行动算子
 * 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
 */
object RDD_Action_reduce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // rdd内元素累加
    val i: Int = rdd.reduce(_ + _)
    println(i)

    sc.stop()

  }
}
