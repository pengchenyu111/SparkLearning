package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL_Json {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 读取JSON数据
    // 注意：Spark 读取的 JSON 文件不是传统的 JSON 文件，每一行都应该是一个 JSON 串
    val df: DataFrame = spark.read.json("datas/user.json")
    df.show()

    spark.close()
  }
}
