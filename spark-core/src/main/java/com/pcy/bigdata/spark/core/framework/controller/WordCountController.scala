package com.pcy.bigdata.spark.core.framework.controller

import com.pcy.bigdata.spark.core.framework.common.TController
import com.pcy.bigdata.spark.core.framework.service.WordCountService


/**
 * 控制层
 */
class WordCountController extends TController {

  private val wordCountService = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    // 执行业务操作
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
