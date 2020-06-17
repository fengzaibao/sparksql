package com.yc.sparksqlmovie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 需求：
 * 需求5: 分析最受不同年龄段人员欢迎的电影的前10
 * 输出格式:   ( movieId, 电影名,平均分)
 * *
 */
/** 需求5: 分析最受不同年龄段人员欢迎的电影的前10
 * 原始数据集中的年龄段划分
 * under 18: 1
 * 18 - 24: 18
 * 25 - 34: 25
 * 35 - 44: 35
 * 45 - 49: 45
 * 50 - 55: 50
 * 56 + 56
 *
 */

/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object sparksqlmovie5 {
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

    var age = "1" //18岁以下的人喜好的电影top10
    println("简版显示:")


    println("年龄为" + age + "的用户量:" + spark.sql("select count(*) from v_users where age=1").collect()(0))
    println("评分数据量为:" + spark.sql("select count(movieid) from v_ratings").collect()(0))
    println("年龄在" + age + "的评分数据量为:" + spark.sql("select count(movieid) from v_ratings inner join v_users on v_ratings.userid=v_users.userid where age=1").collect()(0))

    println("年龄段为:" + age + "喜爱的电影Top 10:(SQL)")
    spark.sql("select v_ratings.movieid, avg(rating) as avgrating " +
      "from v_ratings " +
      "inner join v_users " +
      "on v_users.userid=v_ratings.userid " +
      "where age='1' " +
      "group by v_ratings.movieid " +
      "order by avgrating desc,v_ratings.movieid asc " +
      "limit 10 ").show()

    println("年龄段为:" + age + "喜爱的电影Top 10:(API)")
    ratingsDF.select("MovieID", "rating", "userid")
      // 联接条件中列重名解决方案一：将一个表中的列重命名
      //.withColumnRenamed("userid", "uid")
      //.join(usersDF, $"uid" === $"userid")      // $  =>  根据名字到Dataset找这个列  ->   Column
      // 联接条件中列重名解决方案二：指定Dataframe取列
      .join(usersDF, ratingsDF("userid") === usersDF("userid"))
      .where($"age" === "1") //加入比较条件
      .groupBy("MovieID")
      .avg("rating") //     avg(  avgrating )
      .sort($"avg(rating)".desc, $"MovieID".asc)
      .withColumnRenamed("avg(rating)", "avgrating")
      .select("MovieID", "avgrating")
      .show(10)

    println("详情显示:")
    println("年龄段为:" + age + "喜爱的电影Top 10:(SQL)")
    spark.sql("select v_ratings.movieid,title,genres, avg(rating) as avgrating, count(rating) as cns " +
      "from v_ratings " +
      "inner join v_users " +
      "on v_users.userid=v_ratings.userid " +
      "inner join v_movies " +
      "on v_movies.movieid=v_ratings.movieid " + //错误: 少一个空格
      "where age='1' " +
      "group by v_ratings.movieid,title,genres " + // 错误:groupby
      "order by avgrating desc,cns desc, title asc " + //错误:如果不空格
      "limit 10 ").show()

    println("年龄段为:" + age + "喜爱的电影Top 10:(API)")
    import org.apache.spark.sql.functions._ //sql中支持的内置函数   (  max, min,avg, count, to_date,........ substr )

    ratingsDF.select("MovieID", "rating", "userid")
      .withColumnRenamed("userid", "uid")
      .withColumnRenamed("movieid", "mid")
      .join(usersDF, $"uid" === $"userid")
      .join(moviesDF, $"mid" === $"movieid")
      .where($"age" === "1") //加入比较条件
      .groupBy("MovieID", "title", "genres")

      .agg(avg(ratingsDF("rating")).as("avgrating"), count(ratingsDF("rating")).as("cns"))
      .sort($"avgrating".desc, $"cns".desc, $"title".asc)
      .select($"movieid", $"title", $"genres", $"avgrating", $"cns")
      .show(10)

    spark.stop()
  }

}
