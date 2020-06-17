package com.yc.sparksql2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
具体数据结构如下所示：
* User
* |-- name: string (nullable = true)
* |-- registeredTime: string (nullable = true)
* |-- userID: long (nullable = true)
* *
* Log
* |-- consumed: double (nullable = true)
* |-- logID: long (nullable = true)
* |-- time: string (nullable = true)
* |-- typed: long (nullable = true)
* |-- userID: long (nullable = true)

需求:   1. 读取数集  Mock_EB_Log_Data.log,Mock_EB_Log_Data.log,构建DataFrame
       2. 利用 sql方式或是 DataFrame Api方式完成以下需求:
            a. 特定时间段内用户访问电商网站排名Top 5 个用户 ( 2020-01-01 至 2020-01-15 日 )
            b. 统计特定时间段内用户购买总金额排名Top 5 个用户   ( 2020-01-01 至 2020-01-15 日 )
            c. 统计特定时间段内用户访问次数增长排名Top 5个用户, 例如说这一周比上一周访问次数增长最快的5位用户
            d. 统计特定时间段里购买金额增长最多的Top5用户，例如说这一周比上一周访问次数增长最快的5位用户
            c. 统计注册之后前两周内访问最多的前10个人
            d. 统计注册之后前两周内购买总额最多的Top 10

*/

object Test1_EbusinessAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val filepath = "data/ebusiness/"
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val userInfo = spark.read.format("json").
      json(filepath + "Users_Data.log")
    val userLog = spark.read.format("json").
      json(filepath + "Log_Data.log")

    println("用户信息及用户访问记录文件JSON格式 :")
    userInfo.printSchema()
    userLog.printSchema()

    //数据处理



    spark.stop()
  }
}
