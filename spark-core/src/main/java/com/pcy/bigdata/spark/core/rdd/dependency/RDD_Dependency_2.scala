package com.pcy.bigdata.spark.core.rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 依赖关系：两个相邻 RDD 之间的关系
 *
 * 注意：是Partition
 *
 * 1) RDD 窄依赖   OneToOneDependency
 * 窄依赖表示每一个父(上游)RDD 的 Partition 最多被子（下游）RDD 的一个 Partition 使用，窄依赖我们形象的比喻为独生子女。
 * class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd)
 *
 *
 * 2) RDD 宽依赖   ShuffleDependency
 * 宽依赖表示同一个父（上游）RDD 的 Partition 被多个子（下游）RDD 的 Partition 依赖，会引起 Shuffle，总结：宽依赖我们形象的比喻为多生。
 * class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag]() extends Dependency[Product2[K, V]]
 */
object RDD_Dependency_2 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("*************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("*************************")
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.dependencies)
    println("*************************")
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.dependencies)
    println("*************************")
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)

    sc.stop()

  }
}
