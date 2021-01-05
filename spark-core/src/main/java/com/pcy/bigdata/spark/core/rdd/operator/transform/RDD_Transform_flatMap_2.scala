package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 和模式匹配的使用
 */
object RDD_Transform_flatMap_2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD = rdd.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }

    flatRDD.collect().foreach(println)


    sc.stop()

  }
}
