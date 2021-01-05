package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
 *
 */

object RDD_Transform_filter_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/apache.log")

    val filteredRDD = rdd.filter(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        time.startsWith("17/05/2015")
      }
    )
    val res = filteredRDD.map(_.split(" ")(6))

    res.collect().foreach(println)

    sc.stop()

  }
}
