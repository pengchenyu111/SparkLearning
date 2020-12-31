package com.pcy.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author PengChenyu
 * @since 2020-12-31 20:52:52
 */
object WordCount2 {

  def main(args: Array[String]): Unit = {

    // 1. 准备Spark环境
    //setMaster: 设定Spark环境的位置
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    // 2. 建立和Spark的连接
    val sparkContext = new SparkContext(sparkConf)

    // 3. 实现业务操作
    // 读取文件（参数path可以指向单个文件也可以是文件目录）
    val fileRDD: RDD[String] = sparkContext.textFile("input")
    // 切分单词, 扁平化处理
    //fileRDD.flatMap(line => {line.split(" ")})
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    // 讲分词后的数据结构转换下
    val wordMapRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    // 根据单词分组聚合，reduceByKey表示对key进行分组，对value进行统计聚合
    // val wordToSumRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey((x, y) => {x + y})
    // val wordToSumRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey((x, y) => x + y) 只有一行可省略{}
    // val wordToSumRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey((x, y) => x + y) 参数类型可以推断出来，类型可以不写
    // val wordToSumRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey((x, y) => x + y) 参数只使用一次，则可以使用下划线_代替
    val wordToSumRDD: RDD[(String, Int)] = wordMapRDD.reduceByKey(_ + _)
    // 采集输出
    val wordCountArray: Array[(String, Int)] = wordToSumRDD.collect()
    //println(wordCountArray.mkString(","))
    wordCountArray.foreach(println)


    // 4. 释放连接
    sparkContext.stop()
  }

}
