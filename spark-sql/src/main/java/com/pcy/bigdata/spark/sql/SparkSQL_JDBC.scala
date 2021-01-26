package com.pcy.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 读取MySQL数据

    // 方式1：通过load
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://49.232.218.99:3306/movie_recommendation?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "xxxxxxxx")
      .option("dbtable", "movie_user")
      .load()

    // 方式2: 通用的 load 方法读取参数另一种形式
    val df2: DataFrame = spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://49.232.218.99:3306/movie_recommendation?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=true&user=root&password= xxxxxxxx",
          "dbtable" -> "movie_user",
          "driver" -> "com.mysql.cj.jdbc.Driver")
      )
      .load()

    // 方式3: 使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "xxxxxxxx")
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    val df3: DataFrame = spark.read.jdbc("jdbc:mysql://49.232.218.99:3306/movie_recommendation?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=true", "movie_user", props)


    df.show(10)

    spark.close()
  }
}
