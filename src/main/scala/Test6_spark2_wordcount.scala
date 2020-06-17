import org.apache.spark.sql.{Dataset, SparkSession}

object Test6_spark2_wordcount {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val lines:Dataset[String]=spark.read.textFile("data/wc.txt")//Dataset  此时Row只有一个列
    import  spark.implicits._
    val words=lines.flatMap(_.split(" ")) //DataSet
    //DataSet就是DataFrame
    //方案一：sql
    words.createTempView("wc")
    val result=spark.sql("select value,count(*) nums from wc group by value order by nums desc")

    //方案二:Api
    //val result=words.groupBy($"value" as "word").count().sort($"count" desc)
    result.show()
    spark.stop()

  }
}
