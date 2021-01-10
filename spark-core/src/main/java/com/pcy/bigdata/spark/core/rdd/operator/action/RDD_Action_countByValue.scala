package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * countByValue 行动算子
 *
 */
object RDD_Action_countByValue {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 1, 1, 4))

    val result: collection.Map[Int, Long] = rdd.countByValue()
    println(result)

    sc.stop()

  }
}
