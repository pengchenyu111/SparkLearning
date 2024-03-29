package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Transform_mapPartitionsWithIndex_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        // 1,   2,    3,   4
        //(0,1)(2,2),(4,3),(6,4)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpiRDD.collect().foreach(println)


    sc.stop()

  }
}
