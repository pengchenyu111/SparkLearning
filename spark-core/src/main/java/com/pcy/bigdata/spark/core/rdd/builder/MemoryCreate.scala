package com.pcy.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建RDD
 *
 * @author PengChenyu
 * @since 2021-01-02 15:17:54
 */
object MemoryCreate {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val seq = Seq[Int](1, 2, 3, 4)

    // 方法一：使用parallelize
    val rdd = sparkContext.parallelize(seq)

    // 方法二：使用makeRDD,其实底层就是parallelize方法
    val rdd2 = sparkContext.makeRDD(seq)

    rdd.collect().foreach(println)


    sparkContext.stop()
  }

}
