package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * key-value 类型
 * reduceByKey
 * 可以将数据按照相同的 Key 对 Value 进行聚合
 */
object RDD_Transform_reduceByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    // reduceByKey : 相同的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    // 【1，2，3】
    // 【3，3】
    // 【6】
    // reduceByKey中如果key的数据只有一个，是不会参与运算的。
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => x + y)
    val reduceRDD1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    val reduceRDD2: RDD[(String, Int)] = rdd.reduceByKey(_ + _, 2)

    reduceRDD.collect().foreach(println)
    reduceRDD1.collect().foreach(println)
    reduceRDD2.collect().foreach(println)


    sc.stop()

  }
}
