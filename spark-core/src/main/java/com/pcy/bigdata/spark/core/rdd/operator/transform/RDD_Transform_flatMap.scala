package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 基本使用
 */
object RDD_Transform_flatMap {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))

    val flatRDD = rdd.flatMap(
      // 把rdd内的每个元素放到一个新的集合里
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)


    sc.stop()

  }
}
