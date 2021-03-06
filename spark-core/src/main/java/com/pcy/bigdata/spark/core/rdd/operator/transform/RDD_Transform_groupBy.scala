package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy算子
 * 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样
 * 的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中
 * 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
 */
object RDD_Transform_groupBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    // 相同的key值的数据会放置在一个组中
    def groupFunction(num: Int) = {
      num % 2
    }

//    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    groupRDD.collect().foreach(println)


    sc.stop()

  }
}
