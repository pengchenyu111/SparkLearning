package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_select {

  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // RDD <=> DataFrame <=> DataSet 转换需要引入隐式转换规则，否则无法转换
    import spark.implicits._


    // JSON => DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    df.show()

    // DataFrame => SQL方式
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show
    spark.sql("select age, username from user").show
    spark.sql("select avg(age) from user").show
    spark.sql("select avg(age) as avg_age from user").show

    // DataFrame => DSL方式
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    df.select("age", "username").show
    df.select($"age" + 1).show
    df.select('age + 1).show
    df.select('age + 1 as "newage").show

    // 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
