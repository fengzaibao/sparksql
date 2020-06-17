package com.yc.sparksql

import org.apache.spark.sql.SparkSession

object Movie1_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 1")
      .master("local[*]")
      .getOrCreate()

    val occupations=spark.sparkContext.textFile("data/occupations.dat")
    val users=spark.sparkContext.textFile("data/users.dat")

    val userssql=users.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val Gender=frields(1).toString
      val Age=frields(2).toInt
      val OccpationID=frields(3).toInt
      val code=frields(4)
      Users(UserID,Gender,Age,OccpationID,code)
    })
    import spark.implicits._
    val userssqlDF=userssql.toDF()
    val occsql=occupations.map(line=>{
      val frields=line.split("::")
      val OccpationID=frields(0).toInt
      val OccpationIDName=frields(1).toString
      Occupations(OccpationID,OccpationIDName)
    })
    val occsqlDF=occsql.toDF()
    userssqlDF.createTempView("v_user")
    occsqlDF.createTempView("occ")
    val count=spark.sql("select count(UserID)count from v_user")
    println(count.collect()(0).getAs(0))
    //val sparksqldataset=spark.sql("select UserID,Gender,Age,occ.OccpationID,code from v_user inner join occ on occ.OccpationID=v_user.OccpationID")
    //sparksqldataset.show()

     val count1=userssqlDF.withColumnRenamed("OccpationID","oid")
        .join(occsqlDF,$"OccpationID"===$"oid")
        .select("UserID","Gender","Age","code","oid","code")
    count1.show()

    spark.stop()
  }
}
case class Users(UserID:Integer,Gender:String,Age:Integer,OccpationID:Integer,code:String )
case class ratings(UserID:Integer,MovieID:Integer,Rating:String,Timestamp:String)
case class Occupations(OccpationID:Integer,OccpationIDName:String)