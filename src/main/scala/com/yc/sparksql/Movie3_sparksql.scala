package com.yc.sparksql

import org.apache.spark.sql.SparkSession

object Movie3_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 3")
      .master("local[*]")
      .getOrCreate()
    val ratings=spark.sparkContext.textFile("data/ratings.dat")
    val ratingsdataset=ratings.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val MovieID=frields(1).toLong
      val Rating=frields(2).toDouble
      MovieRatings(UserID,MovieID,Rating)
    })
    import spark.implicits._
    val ratingsDF=ratingsdataset.toDF()
    ratingsDF.createTempView("v_ratings")
    //val ratingsDataFrame=spark.sql("select MovieID,avg(Rating) as avgratings from v_ratings group by MovieID order by avgratings desc limit 10")

//    ratingsDF.select("MovieID", "Rating")
//      .groupBy("MovieID")
//      .avg("Rating")
//      .sort($"avg(Rating)".desc, $"MovieID".asc)
//      .withColumnRenamed("avg(rating)", "avgrating")
//      .show(10)
    //ratingsDataFrame.show()

    println("观影人数最多的10部电影")
    //val top10=spark.sql("select count(*) as num,MovieID from v_ratings group by MovieID order by num desc limit 10")
    //top10.show()
    ratingsDF.select("MovieID")
        .groupBy("MovieID")
        .count()
        .sort($"count".desc)
      .withColumnRenamed("count","num")
        .show(10)
    spark.stop()
  }
}
case class MovieRatings(UserID:Integer,MovieID:Long,Rating:Double)