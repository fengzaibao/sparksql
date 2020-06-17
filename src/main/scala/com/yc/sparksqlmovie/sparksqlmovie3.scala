package com.yc.sparksqlmovie
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * 需求：
 *   1. 所有电影中平均得分最高的前10部电影:    ( 分数,电影ID)
 * 2. 观看人数最多的前10部电影:   (观影人数,电影ID)
 */
/**
 * 电影点评系统用户行为分析：用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示：
 * 数据描述：
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * 2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * 3，"movies.dat"：MovieID::Title::Genres
 * 4, "occupations.dat"：OccupationID::OccupationName   一般情况下都会以程序中数据结构Haskset的方式存在，是为了做mapjoin
 */
object sparksqlmovie3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val filepath = "data/moviedata/medium/"
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //1.读取数据
    import spark.implicits._
    //    用这个StructType时， spark.createDataFrame(  RDD, structType   ) 要求 是一个RDD,  所以这里用 spark.sparkContext   ,   而不用   spark.read
    val moviesLinesDataset = spark.sparkContext.textFile(filepath + "movies.dat")
    val occupationsLinesDataset = spark.sparkContext.textFile(filepath + "occupations.dat")
    val ratingsLinesDataset = spark.sparkContext.textFile(filepath + "ratings.dat")
    val usersLinesDataset = spark.sparkContext.textFile(filepath + "users.dat")

    //切分
    val moviesDataset = moviesLinesDataset.map(line => {
      val fields = line.split("::")
      var movieId = fields(0) //这里取出的数据列都当成String看
      var title = fields(1)
      val genres = fields(2)
      //转成对象
      Row(movieId, title, genres) // -> apply()   Row[Tuple]
    })
    val occupationsDataset = occupationsLinesDataset.map(line => {
      val fields = line.split("::")
      var occupationID = fields(0)
      var occupationName = fields(1)
      //转成对象
      Row(occupationID, occupationName) // -> apply()
    })
    //UserID::MovieID::Rating::Timestamp
    val ratingsDataset = ratingsLinesDataset.map(line => {
      val fields = line.split("::")
      var userID = fields(0)
      var movieID = fields(1)
      var rating = fields(2).toInt // TODO: 这个转类型
      var timestamp = fields(1)
      //转成对象
      Row(userID, movieID, rating, timestamp) // -> apply()
    })
    //UserID::Gender::Age::OccupationID::Zip-code
    val usersDataset = usersLinesDataset.map(line => {
      val fields = line.split("::")
      var userID = fields(0)
      var gender = fields(1)
      var age = fields(2)
      var occupationID = fields(3)
      var zipcode = fields(4)
      //转成对象
      Row(userID, gender, age, occupationID, zipcode) // -> apply()      Row[Tuple]    =>  _1,_2,_3,_4
    })

    // StructType方案
    /*
    原来的写法:
    val schema2 = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("height", DoubleType, true),
      StructField("classid",IntegerType, true)
    ))
     */
    val movieSchema = StructType("MovieID::Title::Genres".split("::").map(column => StructField(column, StringType, true)))
    val occupationSchema = StructType("OccupationID::OccupationName".split("::").map(column => StructField(column, StringType, true)))
    //UserID::MovieID::Rating::Timestamp
    val ratingSchema = StructType("UserID::MovieID".split("::").map(column => StructField(column, StringType, true)))
      .add("Rating", IntegerType, true) //注意如果数据类型不一致，则采用   add()函数添加  IntegerType必须与  RDD[Row] 中的类型一致.   var rating = fields(2).toInt    // TODO: 这个转类型
      .add("Timestamp", StringType, true)
    val userSchema = StructType("UserID::Gender::Age::OccupationID::Zipcode".split("::").map(column => StructField(column, StringType, true)))

    //创建DataFrame  def createDataFrame(rowRDD: RDD[Row],schema: StructType)
    val moviesDF = spark.createDataFrame(moviesDataset, movieSchema)
    val occupationsDF = spark.createDataFrame(occupationsDataset, occupationSchema)
    val ratingsDF = spark.createDataFrame(ratingsDataset, ratingSchema)
    var usersDF = spark.createDataFrame(usersDataset, userSchema)

    //需求：
    //1. sql方案
    moviesDF.createTempView("v_movies")
    occupationsDF.createTempView("v_occupation")
    ratingsDF.createTempView("v_ratings")
    usersDF.createTempView("v_users")
    // *   1. 所有电影中平均得分最高的前10部电影:    ( 分数,电影ID)
    println("所有电影中平均得分最高的前10部电影") // movieid asc 因为是  String,所以不是按数字值的大小排，而是ASCII表顺序排。
    spark.sql("select  MovieID,avg(rating) as avgrating from v_ratings group by movieid order by avgrating desc,movieid asc limit 10").show()
    // * 2. 观看人数最多的前10部电影:   (观影人数,电影ID)
    println("观看人数最多的前10部电影")
    spark.sql("select movieid,count(movieid) as  countmovie from v_ratings group by movieid order by countmovie desc,movieid asc limit 10").show()

    //2. API方案
    // *   1. 所有电影中平均得分最高的前10部电影:    ( 分数,电影ID)
    println("所有电影中平均得分最高的前10部电影")
    ratingsDF.select("MovieID", "rating")
      .groupBy("MovieID")
      .avg("rating")
      .sort($"avg(rating)".desc, $"movieid".asc)
      .withColumnRenamed("avg(rating)", "avgrating")
      .show(10)
    println("观看人数最多的前10部电影")
    ratingsDF.select("MovieID", "rating")
      .groupBy("MovieID")
      .count()
      .sort($"count".desc, $"movieid".asc)
      .withColumnRenamed("count", "counts")
      .show(10)

    //FIXME:
    //TODO: 更详细的输出信息:关联查询
    //1. sql方案
    println("=======更详细的输出信息========")
    // *   1. 所有电影中平均得分最高的前10部电影:    ( 分数,电影ID)
    println("所有电影中平均得分最高的前10部电影")
    spark.sql("select  v_ratings.MovieID,title, Genres,avg(rating) as avgrating from v_ratings inner join v_movies on v_movies.movieid=v_ratings.movieid group by v_ratings.movieid,title,Genres order by avgrating desc,v_ratings.movieid asc limit 10").show()
    // * 2. 观看人数最多的前10部电影:   (观影人数,电影ID)
    println("观看人数最多的前10部电影")
    spark.sql("select v_ratings.movieid,title, Genres,count(v_ratings.movieid) as  countmovie from v_ratings inner join v_movies on v_movies.movieid=v_ratings.movieid  group by v_ratings.movieid,title,Genres order by countmovie desc,v_ratings.movieid asc limit 10").show()

    //2. API方案
    println("所有电影中平均得分最高的前10部电影")
    ratingsDF.select("MovieID", "rating")
      .withColumnRenamed("MovieID", "mid")
      .join(moviesDF, $"mid" === $"movieid")
      .groupBy("MovieID", "title", "genres")
      .avg("rating")
      .sort($"avg(rating)".desc, $"MovieID".asc)
      .withColumnRenamed("avg(rating)", "avgrating")
      .select("MovieID", "title", "genres", "avgrating")
      .show(10)

    println("观看人数最多的前10部电影")
    ratingsDF.select("MovieID", "rating")
      .withColumnRenamed("MovieID", "mid")
      .join(moviesDF, $"mid" === $"movieid")
      .groupBy("MovieID", "title", "genres")
      .count()
      .sort($"count".desc, $"MovieID".asc)
      .withColumnRenamed("count", "cns")
      .select("MovieID", "title", "genres", "cns")
      .show(10)

    spark.stop()
  }
}
