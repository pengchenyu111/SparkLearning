package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL_Hive {

  def main(args: Array[String]): Unit = {

    // 解决权限问题，root 为hadoop 用户名称
    System.setProperty("HADOOP_USER_NAME", "root")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-size.xml文件到resource目录下下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系（包含MySQL驱动）
    spark.sql("show tables").show

    spark.close()
  }
}
