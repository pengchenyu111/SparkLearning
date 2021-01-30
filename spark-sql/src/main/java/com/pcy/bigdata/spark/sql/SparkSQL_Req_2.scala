package com.pcy.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * 计算各个区域前三大热门商品
 */
object SparkSQL_Req_2 {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use atguigu")

    // rank() over(partition by A order by B) 的意思是按照A进行分组，分组里面的数据按照B进行排序，over即在什么之上，
    // rank()即跳跃排序（比如存在两个第一名，接下来就是第三名）
    // dense_rank():  连续排序，如果有两个第一级时，接下来仍然是第二级
    spark.sql(
      """
        |select
        |    *
        |from (
        |    select
        |        *,
        |        rank() over( partition by area order by clickCnt desc ) as rank
        |    from (
        |        select
        |           area,
        |           product_name,
        |           count(*) as clickCnt
        |        from (
        |            select
        |               a.*,
        |               p.product_name,
        |               c.area,
        |               c.city_name
        |            from user_visit_action a
        |            join product_info p on a.click_product_id = p.product_id
        |            join city_info c on a.city_id = c.city_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3 where rank <= 3
            """.stripMargin).show


    spark.close()
  }
}
