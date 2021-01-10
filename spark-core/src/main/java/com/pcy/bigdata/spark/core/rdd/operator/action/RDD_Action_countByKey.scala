package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * countByKey 行动算子
 * 统计每种 key 的个数
 */
object RDD_Action_countByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    sc.stop()

  }
}
