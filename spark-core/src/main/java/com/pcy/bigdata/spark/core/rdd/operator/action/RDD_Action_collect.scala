package com.pcy.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Action_collect {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 所谓的行动算子，其实就是触发作业(Job)执行的方法
    // 底层代码调用的是环境对象的runJob方法
    // 底层代码中会创建ActiveJob，并提交执行。

    // collect : 方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))

    sc.stop()

  }
}
