package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Action_save {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    // saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("output2")

    sc.stop()

  }
}
