package com.pcy.bigdata.spark.core.framework.common

import com.pcy.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  /**
   *
   * @param master 模式，默认：local[*]
   * @param app    应用程序名，默认：Application
   * @param op     传入的调度逻辑
   */
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
    val sparConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    // 关闭连接
    sc.stop()
    EnvUtil.clear()
  }
}
