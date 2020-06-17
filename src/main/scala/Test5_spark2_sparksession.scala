import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * sprak2.x的创建dataframe方案
 */
object Test5_spark2_sparksession {
  def main(args: Array[String]): Unit = {
      val spark=SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .getOrCreate()

    //val lines:Dataset[String]=spark.read.textFile("data/person.txt")//Dataset
    val lines=spark.sparkContext.textFile("data/person.txt") //RDD[String]
    val personRDD=lines.map(line=>{
      val fields=line.split(",")
      val id=fields(0).toInt
      val name=fields(1)
      val age=fields(2).toInt
      val height=fields(3).toDouble

      Row(id,name,age,height)
    })
    //schema的创建
    val schema=StructType(List(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("height",DoubleType,true)
    ))
    val df=spark.createDataFrame(personRDD,schema)
    //spark1.x用sparkContext创建DataFrame
    //spark2.x用sparksession创建DataFrame
    // val df=sqlContext.createDataFrame(personRDD,schema)
    //得到DataFrame后，有两种操作1.SQL 2.API
    //df.registerTempTable("person")
    //val resultDataFrame=spark.sql("select * from person order by age desc,height desc,name asc")

    //2.api(DSL  -> $字段名转为ColumnName对象，需要隐式转换)
    //为什么之前是import sqlContext.implicits._  现在是import spark.implicits._
    import spark.implicits._
    val resultDataFrame=df.select("id","name","age","height").where($"age">10).orderBy($"age" desc,$"height"desc,$"name" asc)
    resultDataFrame.show()


    spark.stop()
  }
}
