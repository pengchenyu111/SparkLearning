package com.pcy.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 电影点评系统案例
 *
 * @author PengChenyu
 * @since 2021-09-22 14:33:34
 */
object MovieUserAnalyzer {
  val dataPath = "datas/ml-1m/"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MovieUserAnalyzer")
    val sparkContext = new SparkContext(sparkConf)

    val userRDD: RDD[String] = sparkContext.textFile(dataPath + "users.dat")
    val movieRDD: RDD[String] = sparkContext.textFile(dataPath + "movies.dat")
    val ratingRDD: RDD[String] = sparkContext.textFile(dataPath + "ratings.dat")
    val occupationRDD: RDD[String] = sparkContext.textFile(dataPath + "occupations.dat")

    //  （职业 ID, （用户 ID ，性别，年龄））
    val userBasic: RDD[(String, (String, String, String))] = userRDD
      .map(_.split("::"))
      .map {
        x => (x(3), (x(0), x(1), x(2)))
      }

    // （职业id，职业名）
    val occupations: RDD[(String, String)] = occupationRDD
      .map(_.split("::"))
      .map {
        x => (x(0), x(1))
      }

    // 整合user 和 occupation
    //  (职业ID, ( （用户ID ，性别，年龄） , 职业名））)
    val userInformation: RDD[(String, ((String, String, String), String))] = userBasic.join(occupations)
    userInformation.cache()

    // (用户ID，电影ID)
    val targetMovies: RDD[(String, String)] = ratingRDD
      .map(_.split("::"))
      .map { x => (x(0), x(1)) }
      .filter(_._2.equals("1193"))

    // (用户ID，((用户ID，性别，年龄)，职业名))
    val targetUsers: RDD[(String, ((String, String, String), String))] = userInformation
      .map(x => (x._2._1._1, x._2))

    // (用户id，电影id，性别，年龄，职业名)
    // 计算观看电影id为1193的用户信息
    val userInformationForSpecialMovie: RDD[(String, String, String, String, String)] = targetMovies.join(targetUsers)
      .map {
        x => (x._1, x._2._1, x._2._2._1._2, x._2._2._1._3, x._2._2._2)
      }
    userInformationForSpecialMovie.collect().foreach(println)
  }


}
