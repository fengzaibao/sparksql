package com.yc.sparksqlmovie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 需求一:
 *     1. 读取信息,统计数据条数.   职业数, 电影数, 用户数, 评分条数
 *     2. 显示 每个职业 下的用户详细信息    显示为:  (   职业编号,( 人的编号,性别,年龄,邮编 ), 职业名)
 */
/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object sparksqlmovie {
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
    val moviesLinesDataset: Dataset[String] = spark.read.textFile(filepath+"movies.dat")
    val occupationsLinesDataset: Dataset[String] = spark.read.textFile(filepath+"occupations.dat")
    val ratingsLinesDataset: Dataset[String] = spark.read.textFile(filepath+"ratings.dat")
    val usersLinesDataset: Dataset[String] = spark.read.textFile(filepath+"users.dat")

    val moviesDataset = moviesLinesDataset.map(line => {
      val fields = line.split("::")
      var movieId=fields(0).toLong
      var title=fields(1).toString
      val genres = fields(2).toString
      //转成对象
      Movie(movieId,title, genres) // -> apply()
    })
    //OccupationID::OccupationName
    val occupationsDataset = occupationsLinesDataset.map(line => {
      val fields = line.split("::")
      var occupationID=fields(0).toLong
      var occupationName=fields(1).toString
      //转成对象
      Occupation(occupationID,occupationName) // -> apply()
    })
    //UserID::MovieID::Rating::Timestamp
    val ratingsDataset = ratingsLinesDataset.map(line => {
      val fields = line.split("::")
      var userID=fields(0).toLong
      var movieID=fields(1).toLong
      var rating=fields(2).toInt
      var timestamp=fields(1).toLong
      //转成对象
      Ratings(userID,movieID,rating,timestamp) // -> apply()
    })
    //UserID::Gender::Age::OccupationID::Zip-code
    val usersDataset = usersLinesDataset.map(line => {
      val fields = line.split("::")
      var userID=fields(0).toLong
      var gender=fields(1).toString
      var age=fields(2).toInt
      var occupationID=fields(3).toLong
      var zipcode=fields(4).toString
      //转成对象
      User(userID,gender,age,occupationID,zipcode) // -> apply()
    })

    //2.转为 DataFrame
    val moviesDF=moviesDataset.toDF()
    val occupationsDF=occupationsDataset.toDF()
    val ratingsDF=ratingsDataset.toDF()
    var usersDF=usersDataset.toDF()
    //3.SQL
    moviesDF.createTempView("v_movies")
    occupationsDF.createTempView(   "v_occupation")
    ratingsDF.createTempView("v_ratings")
    usersDF.createTempView( "v_users")

    // 1. 读取信息,统计数据条数.   职业数, 电影数, 用户数, 评分条数
    println(  "评分条数"+ spark.sql("select count(  userid  ) as cn from v_ratings ").collect().toList )
    println(  "职业条数"+ spark.sql("select count(  occupationID  ) as cn from v_occupation ").collect().toList )
    println(  "电影条数"+ spark.sql("select count(  MovieID  ) as cn from v_movies ").collect().toList )
    println(  "用户条数"+ spark.sql("select count(  userID  ) as cn from v_users ").collect().toList )
    //2. 显示 每个职业 下的用户详细信息    显示为:  (   职业编号,( 人的编号,性别,年龄,邮编 ), 职业名)
    val result=spark.sql("select v_occupation.occupationid, occupationName,v_users.userID,gender,age, zipcode from v_users inner join v_occupation on v_occupation.occupationID=v_users.occupationID")
      .toDF()
    println( "每个职业 下的用户详细信息" )
    result.show(50)

    //4. API.
    val result2=usersDF.withColumnRenamed("occupationid","oid")
      .join(  occupationsDF,$"oid"===$"occupationid" )
      .select(    "occupationid","occupationName","userid","gender","age","zipcode" ).toDF()
    println("第二种方法(api+DSL):")
    result2.show( 50 )

    spark.stop()
  }

}
case class Ratings(userID: Long, movieID: Long, rating :Integer, timestamp:Long)
case class User(userID:Long, gender:String,age:Integer,occupationID:Long,zipcode:String)
case class Movie(MovieID:Long, Title:String,Genres:String)
case class Occupation(occupationID:Long,occupationName : String)
