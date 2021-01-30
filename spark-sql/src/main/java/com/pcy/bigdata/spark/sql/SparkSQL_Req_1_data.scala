package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL_Req_1_data {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        spark.sql("use atguigu")

        // 准备数据
        // scala中stripMargin默认是“|”作为出来连接符，在多行换行的行头前面加一个“|”符号即可。
        spark.sql(
            """
              |CREATE TABLE `user_visit_action`(
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'datas/user_visit_action.txt' into table atguigu.user_visit_action
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `product_info`(
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'datas/product_info.txt' into table atguigu.product_info
            """.stripMargin)

        spark.sql(
            """
              |CREATE TABLE `city_info`(
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'datas/city_info.txt' into table atguigu.city_info
            """.stripMargin)

        spark.sql("""select * from city_info""").show


        spark.close()
    }
}
