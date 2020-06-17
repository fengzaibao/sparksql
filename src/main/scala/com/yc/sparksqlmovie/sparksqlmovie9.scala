package com.yc.sparksqlmovie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

/*
需求9: 分析不同职业对观看电影类型的影响
格式:  (职业名,(电影类型,观影次数))
*/
object sparksqlmovie9 {
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

    /*
       分析不同职业对观看电影类型的影响
       格式:  (职业名,(电影类型,观影次数))      1vN:    hive,oracle,mysql自定义函数  UDTF
    */
    val moviesWithGenres = moviesDF.flatMap(row => {
      val movieid = row.getLong(0)
      val title = row.getString(1)
      val genres = row.getString(2)
      val types = genres.split("\\|")
      for (i <- 0 until types.length) yield (movieid, title, types(i))
    })

    //spark.sql("select * from v_users").show()

    moviesWithGenres.toDF("movieid", "title", "genre").createTempView("v_moviesWithGenres")
    spark.sql("select OccupationName,genre,count(genre) " +
      "from v_moviesWithGenres " +
      "inner join v_ratings " +
      "on v_ratings.movieid=v_moviesWithGenres.movieid " +

      "inner join v_users " +
      "on v_users.userid=v_ratings.userid " + //注意这个顺序的问题,这个v_users 不能放在后面，因为inner join按从上到下的顺序.

      "inner join v_occupation " +
      "on v_occupation.occupationID=v_users.occupationID " +


      "group by v_occupation.OccupationID,OccupationName, v_moviesWithgenres.genre " +
      "order by OccupationName desc, genre desc ")
      .show()


    println(" 不同职业对观看电影类型的影响   api方案")
    val moviesWithGenre = moviesWithGenres.toDF("movieid", "title", "genre")
    moviesWithGenre.join(ratingsDF, moviesWithGenre("movieid") === ratingsDF("movieid"))
      .join(usersDF, ratingsDF("userid") === usersDF("userid"))
      .join(occupationsDF, occupationsDF("occupationid") === usersDF("occupationid"))
      .groupBy(occupationsDF("occupationid"), occupationsDF("OccupationName"), $"genre")
      .count()
      .orderBy($"OccupationName".desc, $"genre".desc)
      .show()


    spark.stop()
  }
}
