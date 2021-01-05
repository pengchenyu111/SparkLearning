package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitionsWithIndex 操作指定分区内的数据
 */
object RDD_Transform_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 【1，2】，【3，4】
    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    mpiRDD.collect().foreach(println)


    sc.stop()

  }
}
