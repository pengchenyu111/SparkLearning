package com.pcy.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过文件创建分区，分区的数量的计算方法
 */
object FileParallelize {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    //val rdd = sc.textFile("datas/1.txt")
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数

    // Spark读取文件，底层其实使用的就是Hadoop的读取方式：FileInputFormat 的 getSplits 方法中
    // 分区数量的计算方式：按字节切分的
    //    totalSize = 7
    //    goalSize =  7 / 2 = 3（byte）

    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区)

    //
    val rdd = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")


    sc.stop()
  }
}
