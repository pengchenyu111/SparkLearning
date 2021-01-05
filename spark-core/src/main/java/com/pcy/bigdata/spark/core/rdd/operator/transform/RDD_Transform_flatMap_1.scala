package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 基本使用
 */
object RDD_Transform_flatMap_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    //      rdd.flatMap(
    //          s => {
    //              s.split(" ")
    //          }
    //      )
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    flatRDD.collect().foreach(println)


    sc.stop()

  }
}
