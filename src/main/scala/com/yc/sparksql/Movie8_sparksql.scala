package com.yc.sparksql

import java.util.regex.Pattern

import com.yc.sparksqlmovie.{Movie, Occupation, Ratings, User}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Movie8_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 8")
      .master("local[*]")
      .getOrCreate()
    val moviesLinesDataset=spark.sparkContext.textFile("data/movies.dat")
    val occupationsLinesDataset=spark.sparkContext.textFile("data/occupations.dat")
    val usersLinesDataset=spark.sparkContext.textFile("data/users.dat")
    val ratingsLinesDataset=spark.sparkContext.textFile("data/ratings.dat")

    import spark.implicits._

    val years=moviesLinesDataset.map(line=>{
      val fields=line.split("::")
      val title=fields(1)
      val genres=fields(2)
      MovieInfos(title,genres)
    })
      .map(x=>{
        val x2=x.Genres.split("\\|")
        (x.Title,x2)
      })
      .map(item=>{
        var year = ""
        val pattern = Pattern.compile(" (.*) (\\(\\d{4}\\))") // Toy Story (1995)      (.*) (\\(\\d{4}\\))
        val matcher = pattern.matcher(item._1)
        if (matcher.find()) {
          year = matcher.group(2)
          year = year.substring(1, year.length() - 1)
        }
      (year,item._2)
      })

    val schema=StructType(List(
      StructField("year",StringType,true) ,
        StructField("type",Array,true)
    )
    )
    import spark.implicits._
    val yearsdf=spark.createDataFrame(years,schema)
    val yearDF=yearsdf.toDF("year")
    yearDF.createTempView("v_movies")

    spark.sql("select year,movietype(genres)as movietype from v_movies group by year,movietype(genres) order by year asc")
      .show()
    spark.stop()
  }
}
case class UserInfo(userID:Long, gender:String,age:Integer,occupationID:Long,zipcode:String)
case class MovieInfos( Title:String,Genres:String)
case class OccupationInfo(occupationID:Long,occupationName : String)