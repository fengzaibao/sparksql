
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object Test4_SQLContext_structType{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("spark sql one").setMaster("local[*]")
    val sc=new SparkContext(conf)

    //创建SQLContext对象
    val sqlContext=new SQLContext(sc)

    val lines=sc.textFile("data/person.txt")
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
       )
    )
    val df=sqlContext.createDataFrame(personRDD,schema)

    //得到DataFrame后，有两种操作1.SQL 2.DataFrame的API(DSL)
    import  sqlContext.implicits._
    val resultDataFrame=df.select("id","age","height").orderBy($"age" desc ,$"height"desc,$"name" asc)

    resultDataFrame.show()


    sc.stop()

  }
}
