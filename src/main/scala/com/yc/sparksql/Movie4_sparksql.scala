package com.yc.sparksql

import org.apache.spark.sql.SparkSession

object Movie4_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 4")
      .master("local[*]")
      .getOrCreate()
    val ratings=spark.sparkContext.textFile("data/ratings.dat")
    val users=spark.sparkContext.textFile("data/users.dat")
    val ratingsdataset=ratings.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val MovieID=frields(1).toLong
      val Rating=frields(2).toDouble
      RatingsInfo(UserID,MovieID,Rating)
    })
    val userssql=users.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val Gender=frields(1).toString
      val Age=frields(2).toInt
      val OccpationID=frields(3).toInt
      val code=frields(4)
      UsersInfo(UserID,Gender,Age,OccpationID,code)
    })
    import spark.implicits._
    val User=userssql.toDF
    val Rating=ratingsdataset.toDF()
    User.createTempView("v_users")
    Rating.createTempView("v_rating")
    println("男性评分总分最高的10部电影, 格式：  总评分,电影ID")
    //val mendataframe=spark.sql("select MovieID,sum(Rating)as countrating from v_rating inner join v_users on v_users.UserID=v_rating.UserID where Gender='M' group by MovieID order by countrating desc limit 10")


      //.join(usersDF, $"uid" === $"userid")      // $  =>  根据名字到Dataset找这个列  ->   Column
      // 联接条件中列重名解决方案二：指定Dataframe取列
      //.join(usersDF, ratingsDF("userid") === usersDF("userid"))
//    Rating.select("MovieID","UserID","Rating")
//      .withColumnRenamed("UserID","uid")
//        .join(User,$"uid"===$"UserID")
//        .where($"gender" === "M")
//        .groupBy("MovieID")
//        .sum("Rating")
//        .sort($"sum(Rating)".desc)
//      .withColumnRenamed("sum(Rating)","sumrating")
//      .select("MovieID","sumrating")
//        .show(10)

    println("男性评分平均分最高的10部电影的详情")
    val mendataframe=spark.sql("select MovieID,avg(Rating)as avgrating from v_rating inner join v_users on v_users.UserID=v_rating.UserID where Gender='M' group by MovieID order by avgrating desc limit 10")
    mendataframe.show()
    spark.stop()
  }
}
case class RatingsInfo(UserID:Integer,MovieID:Long,Rating:Double)
case class UsersInfo(UserID:Integer,Gender:String,Age:Integer,OccpationID:Integer,code:String )