package com.pcy.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从文件中创建RDD
 *
 * @author PengChenyu
 * @since 2021-01-02 15:17:54
 */
object FileCreate {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    // textFile以行为单位读取数据
    // path路径默认以当前环境的根路径为基准
    val rdd = sparkContext.textFile("datas/1.txt")

    // path路径可以是文件的具体路径，也可以是目录名，即可以读取多个文件
    val rdd2 = sparkContext.textFile("datas")

    // path路径可以使用通配符
    val rdd3 = sparkContext.textFile("datas/1*.txt")

    // path路径可以是分布式存储系统的路径：HDFS
    val rdd4 = sparkContext.textFile("hdfs://linux:8020/test.txt")


    // wholeTextFiles以文件为单位读取数据
    // 读取的数据表示为元组，(文件路径，文件内容)
    val rdd5 = sparkContext.wholeTextFiles("datas")


    rdd5.collect().foreach(println)

    sparkContext.stop()
  }

}
