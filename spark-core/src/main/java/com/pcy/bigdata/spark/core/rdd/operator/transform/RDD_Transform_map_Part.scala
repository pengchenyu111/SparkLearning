package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分区不变，数据开始再哪个分区，转换后仍在哪个分区
 */
object RDD_Transform_map_Part {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)
        // 【1，2】，【3，4】
        rdd.saveAsTextFile("output")
        val mapRDD = rdd.map(_*2)
        // 【2，4】，【6，8】
        mapRDD.saveAsTextFile("output1")

        sc.stop()

    }
}
