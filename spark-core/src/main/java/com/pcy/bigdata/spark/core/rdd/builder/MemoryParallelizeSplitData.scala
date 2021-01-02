package com.pcy.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分区时怎样切分数据的
 */
object MemoryParallelizeSplitData {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // ParallelCollectionRDD 的 positions方法中 中可找到算法
    // 【1，2】，【3，4】
    //val rdd = sc.makeRDD(List(1,2,3,4), 2)
    // 【1】，【2】，【3，4】
    //val rdd = sc.makeRDD(List(1,2,3,4), 3)
    // 【1】，【2,3】，【4,5】
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
