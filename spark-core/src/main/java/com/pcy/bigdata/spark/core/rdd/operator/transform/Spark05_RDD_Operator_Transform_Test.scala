package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
 */
object Spark05_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 【1，2】，【3，4】
    // 【2】，【4】
    // 【6】
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    //    val maxRDD: RDD[Int] = glomRDD.map(
    //      array => {
    //        array.max
    //      }
    //    )
    val maxRDD = glomRDD.map(_.max)
    println(maxRDD.collect().sum)


    sc.stop()

  }
}
