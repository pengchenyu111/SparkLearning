package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL_CSV {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 读取CSV数据
    val df: DataFrame = spark.read.format("csv")
      .option("sep", ";") // 分隔符
      .option("inferSchema", "true")
      .option("header", "true") // 是否有表头
      .load("datas/user.csv")


    df.show()

    spark.close()
  }
}
