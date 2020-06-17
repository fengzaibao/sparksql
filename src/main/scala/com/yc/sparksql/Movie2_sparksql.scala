package com.yc.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Movie2_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie2")
      .master("local[*]")
      .getOrCreate()
    var userId = "18"
    val ratings=spark.sparkContext.textFile("data/ratings.dat")

    val ratingsdataset=ratings.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val MovieID=frields(1).toLong
      Ratings(UserID,MovieID)
    })
    import spark.implicits._
    val ratingsDF=ratingsdataset.toDF()
    //ratingsDF.createTempView("v_ratings")
    //val count=spark.sql("select count(UserID) count from v_ratings where UserID=18")
    //println(count.collect()(0).getAs(0))
    spark.stop()
  }
}
case class Ratings(UserID:Integer,MovieID:Long)