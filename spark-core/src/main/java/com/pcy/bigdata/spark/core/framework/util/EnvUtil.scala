package com.pcy.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * 用于创建环境
 */
object EnvUtil {

  private val scLocal = new ThreadLocal[SparkContext]()

  /**
   * 放数据
   *
   * @param sc
   */
  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  /**
   * 取数据
   *
   * @return
   */
  def take(): SparkContext = {
    scLocal.get()
  }

  /**
   * 清除数据
   */
  def clear(): Unit = {
    scLocal.remove()
  }
}
