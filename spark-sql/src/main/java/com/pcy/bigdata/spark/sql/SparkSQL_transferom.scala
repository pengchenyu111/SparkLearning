package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_transferom {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._


    // DataSet
    // DataFrame其实是特定泛型的DataSet
    //val seq = Seq(1,2,3,4)
    //val ds: Dataset[Int] = seq.toDS()
    //ds.show()

    // RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.show()
    // 转回RDD注意类型变为Row
    val rowRDD: RDD[Row] = df.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    ds.show()
    val df1: DataFrame = ds.toDF()
    df1.show()

    // RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD: RDD[User] = ds1.rdd


    // 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
