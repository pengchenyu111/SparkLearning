package com.pcy.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过文件创建分区, 数据是怎样被切分的
 */
object FileParallelizeSplitData {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 1. 数据以行为单位进行读取
    //    spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
    // 2. 数据读取时以偏移量为单位,偏移量不会被重复读取
    /*
       1@@   => 012
       2@@   => 345
       3     => 6

     */
    // 3. 数据分区的偏移量范围的计算
    // 0 => [0, 3]  => 1,l2
    // 1 => [3, 6]  => 3
    // 2 => [6, 7]  =>

    // 【1,2】，【3】，【】
    val rdd = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")


    sc.stop()
  }
}
