package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 用户自定义函数
 */
object SparkSQL_UDF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // 用户自定义函数，prefixName为函数名
    // 对每一列中的数据进行一些功能上的拓展
    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })

    spark.sql("select age, prefixName(username) from user").show


    spark.close()
  }
}
