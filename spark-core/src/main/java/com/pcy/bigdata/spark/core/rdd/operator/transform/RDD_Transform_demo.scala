package com.pcy.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 小案例：统计出每一个省份每个广告被点击数量排行的 Top3
 */
object RDD_Transform_demo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")
    // 2. 将原始数据进行结构的转换。方便统计
    //    时间戳，省份，城市，用户，广告
    //    =>
    //    ( ( 省份，广告 ), 1 )
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val temp = line.split(" ")
        ((temp(1), temp(4)), 1)
      }
    )


    // 3. 将转换结构后的数据，进行分组聚合
    //    ( ( 省份，广告 ), 1 ) => ( ( 省份，广告 ), sum )
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    // 4. 将聚合的结果进行结构的转换
    //    ( ( 省份，广告 ), sum ) => ( 省份, ( 广告, sum ) )
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((province, ad), sum) => {
        (province, (ad, sum))
      }
    }


    // 5. 将转换结构后的数据根据省份进行分组
    //    ( 省份, 【( 广告A, sumA )，( 广告B, sumB )】 )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    // 6. 将分组后的数据组内排序（降序），取前3名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    // 7. 采集数据打印在控制台
    resultRDD.collect().foreach(println)

    sc.stop()

  }
}
