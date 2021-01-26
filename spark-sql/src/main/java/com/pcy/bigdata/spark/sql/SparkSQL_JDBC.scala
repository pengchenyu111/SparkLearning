package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 读取MySQL数据
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://49.232.218.99:3306/movie_recommendation")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .load()
    //df.show

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()


    spark.close()
  }
}
