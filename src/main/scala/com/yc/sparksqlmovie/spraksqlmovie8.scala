package com.yc.sparksqlmovie

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

/*
需求8: 分析每年度不同类型的电影生产总数
*/
object spraksqlmovie8{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val filepath = "data/moviedata/medium/"
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    //1.读取数据
    import spark.implicits._
    val moviesLinesDataset: Dataset[String] = spark.read.textFile(filepath + "movies.dat")
    val occupationsLinesDataset: Dataset[String] = spark.read.textFile(filepath + "occupations.dat")
    val ratingsLinesDataset: Dataset[String] = spark.read.textFile(filepath + "ratings.dat")
    val usersLinesDataset: Dataset[String] = spark.read.textFile(filepath + "users.dat")

    val moviesDataset = moviesLinesDataset.map(line => {
      val fields = line.split("::")
      var movieId = fields(0).toLong
      var title = fields(1).toString
      val genres = fields(2).toString
      //转成对象
      Movie(movieId, title, genres) // -> apply()
    })
    //OccupationID::OccupationName
    val occupationsDataset = occupationsLinesDataset.map(line => {
      val fields = line.split("::")
      var occupationID = fields(0).toLong
      var occupationName = fields(1).toString
      //转成对象
      Occupation(occupationID, occupationName) // -> apply()
    })
    //UserID::MovieID::Rating::Timestamp
    val ratingsDataset = ratingsLinesDataset.map(line => {
      val fields = line.split("::")
      var userID = fields(0).toLong
      var movieID = fields(1).toLong
      var rating = fields(2).toInt
      var timestamp = fields(1).toLong
      //转成对象
      Ratings(userID, movieID, rating, timestamp) // -> apply()
    })
    //UserID::Gender::Age::OccupationID::Zip-code
    val usersDataset = usersLinesDataset.map(line => {
      val fields = line.split("::")
      var userID = fields(0).toLong
      var gender = fields(1).toString
      var age = fields(2).toInt
      var occupationID = fields(3).toLong
      var zipcode = fields(4).toString
      //转成对象
      User(userID, gender, age, occupationID, zipcode) // -> apply()
    })

    //2.转为 DataFrame
    val moviesDF = moviesDataset.toDF()
    val occupationsDF = occupationsDataset.toDF()
    val ratingsDF = ratingsDataset.toDF()
    var usersDF = usersDataset.toDF()
    //3.SQL
    moviesDF.createTempView("v_movies")
    occupationsDF.createTempView("v_occupation")
    ratingsDF.createTempView("v_ratings")
    usersDF.createTempView("v_users")


    //分析每年度生产的电影总数
    println("1. 每年度不同类型的电影生产总数( SQL )")
    spark.udf.register("title2year", (title: String) => {
      var mname = ""
      var year = ""
      val pattern = Pattern.compile(" (.*) (\\(\\d{4}\\))") // Toy Story (1995)      (.*) (\\(\\d{4}\\))
      val matcher = pattern.matcher(title)
      if (matcher.find()) {
        mname = matcher.group(1)
        year = matcher.group(2)
        year = year.substring(1, year.length() - 1)
      }
      if (year == "") {
        -1
      } else {
        year.toInt
      }
    })
    println(   "原始电影有:"+ moviesDF.count()+"条")   // 3883条
    moviesDF.show(2)
    //     1::Toy Story (1995)::Animation|Children's|Comedy   转换 =>
    //   1::Toy Story (1995)::Animation
    //    1::Toy Story (1995)::Children's
    //   1::Toy Story (1995)::Comedy
    val moviesWithGenres = moviesDF.flatMap(row => {
      val movieid=row.getLong(0)
      val title=row.getString(1)
      val genres = row.getString(2)
      val types=genres.split("\\|")
      for(i<-0 until types.length) yield (movieid, title, types(i))
    })
    println("显示按类别拆分后的数据:")
    moviesWithGenres.show(6)
    moviesWithGenres.toDF("movieid","title","genre").createTempView("v_moviesWithGenres")

    println( "分解后的记录数:"+ spark.sql("select count(*) from v_moviesWithGenres").collect()(0) )   //6408

    spark.sql("select title2year(title) as year, genre,count(genre) as cns " +
      "from v_moviesWithGenres " +
      "group by title2year(title),genre " +
      "order by year asc, cns desc ").show(20)

    println("1. 每年度不同类型的电影生产总数( API )")
    val moviesWithGenresDF=moviesWithGenres.toDF("movieid","title","genre")
    moviesWithGenresDF.show(2)
    moviesWithGenresDF.selectExpr(   $"title2year(title)".as("year").toString(),"genre" )
      .groupBy(    $"year",$"genre")
      .count()
      .orderBy( $"year".asc,$"count".desc)
      .show()



    spark.stop()

  }
}