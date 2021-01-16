package com.pcy.bigdata.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Executor其实就是一个JVM，所以在启动时，可以自动分配内存，将任务中的闭包数据放置在内存中，达到共享的目的。
 *
 * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个 Spark 操作使用。
 */
object Bc_use {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (w, c) => {
        // 访问广播变量
        val l: Int = bc.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)


    sc.stop()

  }
}
