package com.pcy.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * filter算子
 * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
 * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现 数据倾斜
 *
 */
object RDD_Transform_filter {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filterRDD.collect().foreach(println)


    sc.stop()

  }
}
