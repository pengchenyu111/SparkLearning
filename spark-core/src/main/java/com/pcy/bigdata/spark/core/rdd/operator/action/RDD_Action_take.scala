package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * take 行动算子
 * 返回一个由 RDD 的前 n 个元素组成的数组
 */
object RDD_Action_take {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // take : 获取N个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    sc.stop()

  }
}
