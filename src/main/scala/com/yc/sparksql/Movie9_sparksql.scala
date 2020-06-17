package com.yc.sparksql

import com.yc.sparksqlmovie.{Movie, Occupation, Ratings, User}
import org.apache.spark.sql.SparkSession

object Movie9_sparksql {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .appName("Movie 8")
        .master("local[*]")
        .getOrCreate()

      val moviesLinesDataset=spark.sparkContext.textFile("data/movies.dat")
      val occupationsLinesDataset=spark.sparkContext.textFile("data/occupations.dat")
      val usersLinesDataset=spark.sparkContext.textFile("data/users.dat")
      val ratingsLinesDataset=spark.sparkContext.textFile("data/ratings.dat")
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
      import spark.implicits._
      //2.转为 DataFrame
      val moviesDF=moviesDataset.toDF()
      val occupationsDF=occupationsDataset.toDF()
      val ratingsDF=ratingsDataset.toDF()
      var usersDF=usersDataset.toDF()


    }
  }
}
case class Ratings(userID: Long, movieID: Long, rating :Integer, timestamp:Long)
case class User(userID:Long, gender:String,age:Integer,occupationID:Long,zipcode:String)
case class Movie(MovieID:Long, Title:String,Genres:String)
case class Occupation(occupationID:Long,occupationName : String)
