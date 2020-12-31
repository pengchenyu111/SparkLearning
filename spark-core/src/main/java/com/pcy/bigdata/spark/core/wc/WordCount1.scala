package com.pcy.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author PengChenyu
 * @since 2020-12-31 20:37:04
 */
object WordCount1 {

  def main(args: Array[String]): Unit = {

    // 1. 准备Spark环境
    //setMaster: 设定Spark环境的位置
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    // 2. 建立和Spark的连接
    val sparkContext = new SparkContext(sparkConf)

    // 3. 实现业务操作
    // 读取文件（参数path可以指向单个文件也可以是文件目录）
    val fileRDD: RDD[String] = sparkContext.textFile("datas")
    // 切分单词, 扁平化处理
    //fileRDD.flatMap(line => {line.split(" ")})
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    // 按单词分组
    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)
    // 数据聚合， (word, Iterable) => (word, count)
    val mapRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => (word, iter.size)
    }
    // 采集输出
    val wordCountArray: Array[(String, Int)] = mapRDD.collect()
    //println(wordCountArray.mkString(","))
    wordCountArray.foreach(println)
    // 4. 释放连接
    sparkContext.stop()
  }

}
