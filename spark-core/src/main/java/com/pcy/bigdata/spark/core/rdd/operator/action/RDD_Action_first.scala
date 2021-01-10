package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * first 行动算子
 * 返回 RDD 中的第一个元素
 */
object RDD_Action_first {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // first : 获取数据源中数据的第一个
    val first = rdd.first()
    println(first)

    sc.stop()

  }
}
