package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Transform_map {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 1,2,3,4
    // 2,4,6,8

    // 转换函数
    def mapFunction(num: Int): Int = {
      num * 2
    }

    //val mapRDD: RDD[Int] = rdd.map(mapFunction)
    //val mapRDD: RDD[Int] = rdd.map((num:Int)=>{num*2})
    //val mapRDD: RDD[Int] = rdd.map((num:Int)=>num*2)
    //val mapRDD: RDD[Int] = rdd.map((num)=>num*2)
    //val mapRDD: RDD[Int] = rdd.map(num=>num*2)
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    sc.stop()

  }
}
