package com.yc.sparksql

import org.apache.spark.sql.SparkSession

/**
 * 1.from 表名
 * 2.group by列名
 * 3.select *
 * 4.order by 列名
 */
object Movie5_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 5")
      .master("local[*]")
      .getOrCreate()
    val ratings=spark.sparkContext.textFile("data/ratings.dat")
    val users=spark.sparkContext.textFile("data/users.dat")
    val movies=spark.sparkContext.textFile("data/movies.dat")
    val age=1
    val ratingsrdd=ratings.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val MovieID=frields(1).toLong
      val Rating=frields(2).toDouble
      RatingsInfos(UserID,MovieID,Rating)
    })
    val usersrdd=users.map(line=>{
      val frields=line.split("::")
      val UserID=frields(0).toInt
      val Gender=frields(1).toString
      val Age=frields(2).toInt
      val OccpationID=frields(3).toInt
      val code=frields(4)
      UsersInfos(UserID,Gender,Age,OccpationID,code)
    })
    val moviesrdd=movies.map(line=>{
      val frields=line.split("::")
      val MovieID=frields(0).toLong
      val Title=frields(1)
      val Genres=frields(2)
      Movies(MovieID,Title,Genres)
    })
    import spark.implicits._
    val User=usersrdd.toDF
    val Rating=ratingsrdd.toDF()
    val MovieDF=moviesrdd.toDF()
    User.createTempView("v_user")
    Rating.createTempView("v_rating")
    MovieDF.createTempView("v_movies")
    val count=spark.sql("select count(*) from v_user where age="+age)
    println("年龄为1的用户量"+count.collect()(0).getAs(0))
    val count2=spark.sql("select count(movieid) from v_rating inner join v_user on v_user.userid=v_rating.userid where age="+age)
    println("年龄为1的评分数据量为:"+count2.collect()(0).getAs(0))
    val count3=spark.sql("select count(movieid) from v_rating")
    println("评分总数据量"+count3.collect()(0).getAs(0))

    println("年龄段为:" + age + "喜爱的电影Top 10:( movieId,(电影名,平均分,总分数,观影次数,类型)")
    //两种条件1.分组时候注意，如果有列也在其中，要不一起分组  group by  v_rating.movieid,genres,title
    //或者2.first(title),first(genres)
    //spark.sql("select v_rating.movieid,first(title) as title,avg(rating) as avgrating,sum(rating),count(*)as count,first(genres) from v_movies inner join v_rating on v_rating.movieid=v_movies.movieid group by v_rating.movieid order by avgrating desc,count desc,title asc")
     // .show()

    spark.sql("select v_rating.movieid,avg(rating) as avgrating,sum(rating)as sumrating,count(*)as count from v_movies inner join v_rating on v_rating.movieid=v_movies.movieid group by v_rating.movieid order by avgrating desc")
      .show()
  }
}
case class RatingsInfos(UserID:Integer,MovieID:Long,Rating:Double)
case class UsersInfos(UserID:Integer,Gender:String,Age:Integer,OccpationID:Integer,code:String )
case class Movies(MovieID:Long,Title:String,Genres:String)