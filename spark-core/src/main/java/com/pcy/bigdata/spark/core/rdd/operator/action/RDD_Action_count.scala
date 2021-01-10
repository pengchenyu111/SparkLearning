package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * count 行动算子
 * 返回 RDD 中元素的个数
 */
object RDD_Action_count {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // count : 数据源中数据的个数
    val cnt = rdd.count()
    println(cnt)

    sc.stop()

  }
}
