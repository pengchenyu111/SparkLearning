package com.pcy.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从服务器日志数据 apache.log 中获取每个时间段访问量
 */
object RDD_Transform_groupBy_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        //time.substring(0, )
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        (hour, 1)
      }
    ).groupBy(_._1)
    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect.foreach(println)


    sc.stop()

  }
}
