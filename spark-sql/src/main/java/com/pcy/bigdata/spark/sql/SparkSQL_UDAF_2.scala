package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

/**
 * 用户自定义聚合函数 - 强类型
 * Spark3.0 版本可以采用强类型的 Aggregator 方式代替 UserDefinedAggregateFunction
 *
 * 以求年龄平均值为例
 */
object SparkSQL_UDAF_2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

    spark.sql("select ageAvg(age) from user").show


    spark.close()
  }


  case class Buff(var total: Long, var count: Long)

  /**
   * 自定义聚合函数类：计算年龄的平均值
   *    1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
   * IN : 输入的数据类型 Long
   * BUF : 缓冲区的数据类型 Buff
   * OUT : 输出的数据类型 Long
   *    2. 重写方法(6)
   */
  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // z & zero : 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区的编码操作，自定义的类就用product
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
