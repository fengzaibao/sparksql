package com.yc.sparksqlmovie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 需求：
 * 1. 某个用户看过的电影数量
 * 2. 这些电影的信息，格式为:  (MovieId,Title,Genres)
 */
/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object sparksqlmovie2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val filepath = "data/moviedata/medium/"
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    var userId = "18"

    import spark.implicits._

    //1.读取数据
    import spark.implicits._
    val moviesLinesDataset: Dataset[String] = spark.read.textFile(filepath + "movies.dat")
    val occupationsLinesDataset: Dataset[String] = spark.read.textFile(filepath + "occupations.dat")
    val ratingsLinesDataset: Dataset[String] = spark.read.textFile(filepath + "ratings.dat")
    val usersLinesDataset: Dataset[String] = spark.read.textFile(filepath + "users.dat")

    /*
       1)直接读取  val dataset=session.read.textFile("")
        2) map( )分解成 Dataset[ 元组 ]
        3)  dataframe=dataset.toDF( 列名)
     */
    val moviesDataset = moviesLinesDataset.map(line => {
      val fields = line.split("::")
      var movieId = fields(0).toLong
      var title = fields(1).toString
      val genres = fields(2).toString
      //转成对象
      (movieId, title, genres) // -> apply()   Row[Tuple]
    })
    //OccupationID::OccupationName
    val occupationsDataset = occupationsLinesDataset.map(line => {
      val fields = line.split("::")
      var occupationID = fields(0).toLong
      var occupationName = fields(1).toString
      //转成对象
      (occupationID, occupationName) // -> apply()
    })
    //UserID::MovieID::Rating::Timestamp
    val ratingsDataset = ratingsLinesDataset.map(line => {
      val fields = line.split("::")
      var userID = fields(0).toLong
      var movieID = fields(1).toLong
      var rating = fields(2).toInt
      var timestamp = fields(1).toLong
      //转成对象
      (userID, movieID, rating, timestamp) // -> apply()
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
      (userID, gender, age, occupationID, zipcode) // -> apply()      Row[Tuple]    =>  _1,_2,_3,_4
    })

    //转成DF
    //2.转为 DataFrame
    val moviesDF = moviesDataset.toDF("movieId", "title", "genres")
    val occupationsDF = occupationsDataset.toDF("occupationID", "occupationName")
    val ratingsDF = ratingsDataset.toDF("userID", "movieID", "rating", "timestamp")
    var usersDF = usersDataset.toDF("userID", "gender", "age", "occupationID", "zipcode")

    //3.SQL
    moviesDF.createTempView("v_movies")
    occupationsDF.createTempView("v_occupation")
    ratingsDF.createTempView("v_ratings")
    usersDF.createTempView("v_users")

    val result = spark.sql("select count(*) from v_ratings where userID=" + userId).collect()(0)

    println(userId + "观看过的电影数: " + result)

    //需求二：  这些电影的信息，格式为:  (MovieId,Title,Genres)
    println(" 用户" + userId + "看过的电影有:")
    spark.sql("select v_movies.movieid, title, genres from v_movies inner join v_ratings on v_ratings.movieid=v_movies.movieid where userid=" + userId + " order by v_movies.movieid").toDF().show()

    //以上两个需求转为 Dataframe API实现
    val result2 = ratingsDF.filter($"userID" === userId).count()
    println(userId + "观看过的电影数: " + result2)
    println(" 用户" + userId + "看过的电影有:")
    moviesDF.withColumnRenamed("movieid", "mid").join(ratingsDF, $"mid" === $"movieid").filter($"userID" === userId).orderBy($"mid".asc).toDF().show()


    spark.stop()
  }
}
