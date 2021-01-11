package com.pcy.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Java 的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也较大。
 * Spark 出于性能的考虑，Spark2.0 开始支持另外一种 Kryo 序列化机制。Kryo 速度是 Serializable 的 10 倍。
 * 当 RDD 在 Shuffle 数据的时候，简单数据类型、数组和字符串类型已经在 Spark 内部使用 Kryo 来序列化.
 *
 *
 * 注意：即使使用 Kryo 序列化，也要继承 Serializable 接口
 */
object RDD_Serializable_Kryo {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[Search]))
    val sc = new SparkContext(sparConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")

    search.getMatch1(rdd).collect().foreach(println)
    //search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 查询对象
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  class Search(query: String) extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }

}
