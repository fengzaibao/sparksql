package com.yc.sparksqlmovie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

/*
需求6: 分析 不同类型的电影总数
输出格式:   ( 类型,数量)
*/
object sparksqlmovie6 {
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

    //不同类型的电影总数  : 要将电影类型由一个变为多个.
    //分析:  方案一:  利用UDF, UDAF, UDTF.    用哪一个????
    //      方案二: 因为这是一______,所以可以使用spark中的flatMap()来代替

    //moviesDF.show()
    val moviesWithGenres = moviesDF.flatMap(row => {
      val genres = row.getString(2)
      genres.split("\\|")
    })
    //moviesWithGenres.show()
    println("不同类型的电影总数( SQL)")
    moviesWithGenres.createTempView("v_moviesWithGenres")
    spark.sql("select value, count(*) as cns from v_moviesWithGenres group by value order by cns desc").show()

    println("不同类型的电影总数( API)")
    import org.apache.spark.sql.functions._ //sql中支持的内置函数   (  max, min,avg, count, to_date,........ substr )
    moviesWithGenres.groupBy($"value").agg(count(moviesWithGenres("value")).as("cns")).orderBy($"cns".desc).show()

    spark.stop()
  }
}