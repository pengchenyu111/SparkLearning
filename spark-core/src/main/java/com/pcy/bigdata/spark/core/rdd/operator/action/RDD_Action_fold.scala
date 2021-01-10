package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * fold 行动算子
 * 折叠操作，aggregate 的简化版操作 ：当分区内和分区间的计算规则相同时
 */
object RDD_Action_fold {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val result = rdd.fold(10)(_ + _)
    println(result)

    sc.stop()

  }
}
