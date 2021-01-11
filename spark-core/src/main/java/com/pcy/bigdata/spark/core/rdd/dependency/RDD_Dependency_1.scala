package com.pcy.bigdata.spark.core.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * RDD间的血缘关系
 *
 * RDD 只支持粗粒度转换，即在大量记录上执行的单个操作。
 * 将创建 RDD 的一系列 Lineage（血统）记录下来，以便恢复丢失的分区。RDD 的 Lineage 会记录 RDD 的元数据信息和转换行为，
 * 当该 RDD 的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的数据分区
 */
object RDD_Dependency_1 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("*************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*************************")
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("*************************")
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.toDebugString)
    println("*************************")
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)

    sc.stop()

  }
}
