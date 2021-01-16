package com.pcy.bigdata.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 的数据读取及数据保存可以从两个维度来作区分：文件格式以及文件系统。
 * 文件格式分为：text 文件、csv 文件、sequence 文件以及 Object 文件
 * 文件系统分为：本地文件系统、HDFS、HBASE 以及数据库
 */
object Spark01_RDD_IO_Save {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      )
    )

    rdd.saveAsTextFile("output1")
    // 对象文件是将对象序列化后保存的文件，采用 Java 的序列化机制。
    rdd.saveAsObjectFile("output2")
    // SequenceFile 文件是 Hadoop 用来存储二进制形式的 key-value 对而设计的一种平面文件(FlatFile)
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
