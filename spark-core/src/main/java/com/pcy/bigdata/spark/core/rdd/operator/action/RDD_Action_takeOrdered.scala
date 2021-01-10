package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * takeOrdered 行动算子
 * 返回该 RDD 排序后的前 n 个元素组成的数组
 */
object RDD_Action_takeOrdered {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


    // takeOrdered : 数据排序后，取N个数据
    val rdd1 = sc.makeRDD(List(4, 2, 3, 1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
    println(ints1.mkString(","))

    sc.stop()

  }
}
