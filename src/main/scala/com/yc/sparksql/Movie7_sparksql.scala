package com.yc.sparksql

import java.util.regex.Pattern

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Movie7_sparksql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Movie 7")
      .master("local[*]")
      .getOrCreate()
    val moviesrdd=spark.sparkContext.textFile("data/movies.dat")
    val years=moviesrdd.map(line=>{
      val fields=line.split("::")
      val title=fields(1)
      title
    })
      .map(item=>{
        var year = ""
        val pattern = Pattern.compile(" (.*) (\\(\\d{4}\\))") // Toy Story (1995)      (.*) (\\(\\d{4}\\))
        val matcher = pattern.matcher(item)
        if (matcher.find()) {
          year = matcher.group(2)
          year = year.substring(1, year.length() - 1)
        }
        Row(year)
      })
    val schema= StructType(List(
      StructField("year", StringType, true)
    ))
    import spark.implicits._
    val yearsdf=spark.createDataFrame(years,schema)
    val yearDF=yearsdf.toDF("year")
    yearDF.createTempView("v_year")
    spark.sql("select year,count(year)as countyear from v_year group by year order by countyear desc").show()
    spark.stop()
  }
}
case class MoviesInfo(year:String)