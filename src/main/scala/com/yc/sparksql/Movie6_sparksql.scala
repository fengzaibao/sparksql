package com.yc.sparksql

import org.apache.spark.sql.SparkSession

object Movie6_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 6")
      .master("local[*]")
      .getOrCreate()
    val movies=spark.sparkContext.textFile("data/movies.dat")
    val moviesrdd=movies.map(line=>{
      val frields=line.split("::")
      val Genres=frields(2)
      Genres
    })
    val moviesrdds=moviesrdd.map(x=>{
      x.split("\\|")
    }).flatMap(x=>{
      x
    })

    moviesrdds.foreach(println)
    import spark.implicits._
    val moviesDF=moviesrdds.toDF("movietype")
    moviesDF.createTempView("v_movies")
    spark.sql("select movietype,count(movietype) from v_movies group by movietype ").show()
  }
}