package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * key-value 类型
 * partitionBy算子
 * 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 */
object RDD_Transform_partitionBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    // RDD => PairRDDFunctions
    // 隐式转换（二次编译）

    // partitionBy根据指定的分区规则对数据进行重分区
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
    newRDD.partitionBy(new HashPartitioner(2))

    newRDD.saveAsTextFile("output")


    sc.stop()

  }
}
