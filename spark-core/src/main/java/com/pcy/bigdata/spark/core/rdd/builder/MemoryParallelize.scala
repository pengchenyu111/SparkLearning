package com.pcy.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD的分区设置
 */
object MemoryParallelize {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    // 手动设置分区
    sparkConf.set("spark.default.parallelism", "5")
    val sparkContext = new SparkContext(sparkConf)

    // RDD的并行度 & 分区
    // makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数可以不传递的，那么makeRDD方法会使用默认值 ： defaultParallelism（默认并行度）
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    // 如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数


    //val rdd = sparkContext.makeRDD(List(1,2,3,4),2)
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    sparkContext.stop()
  }
}
