package com.atguigu.spark.sql.project
import java.util.Properties

import org.apache.spark.sql.SparkSession
/**
  * Created by shkstart on 2021/5/20.
  */
object sqlApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("sqlApp")
      .enableHiveSupport()
      .getOrCreate()
    spark.udf.register("remark", new CityRemarkUDAF)

    // 去执行sql, 从hive查询数据
    spark.sql("use sparksqlproject")

    spark.sql(
      """
        |select
        |    ci.*,
        |    pi.product_name,
        |    uva.click_product_id
        |from user_visit_action uva
        |join product_info pi on uva.click_product_id=pi.product_id
        |join city_info ci on uva.city_id=ci.city_id
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count(*) count,
        |    remark(city_name) remark
        |from t1
        |group by area, product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count,
        |    remark,
        |    rank() over(partition by area order by count desc) rk
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")


    val url="jdbc:mysql://hadoop203:3306/rdd?useUnicode=true&characterEncoding=utf8"
    val table="sparksqlproject"

    val props = new Properties()
    props.put("user","root")
    props.put("password","123456")

    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count,
        |    remark
        |from t3
        |where rk<=3
        |""".stripMargin)
      .coalesce(1)
      .write.mode("overwrite")
      .jdbc(url,table,props)



    // 把结果写入到mysql中

    spark.close()
  }
}
