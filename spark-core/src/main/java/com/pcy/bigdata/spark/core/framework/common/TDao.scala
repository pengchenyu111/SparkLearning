package com.pcy.bigdata.spark.core.framework.common

import com.pcy.bigdata.spark.core.framework.util.EnvUtil

trait TDao {

  /**
   * 文件读取
   *
   * @param path 文件路径
   * @return
   */
  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }


}
