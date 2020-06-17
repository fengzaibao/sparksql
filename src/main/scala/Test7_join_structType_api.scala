
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object Test7_join_structType_api {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Spark SQL wordcount")
      .master("local[*]")
      .getOrCreate()

    val classesLinesDataset=spark.sparkContext.textFile("data/classes.txt")
    //val studentsLinesDataser=spark.read.textFile("data/students.txt")
    val studentsLinesDataser=spark.sparkContext.textFile("data/students.txt")


    //1.不用样例类，而用StructType来完成DataFrame的创建
    val classesRDD=classesLinesDataset.map(line=>{
      val fields=line.split(" ")
      val cid=fields(0).toInt
      val cname=fields(1)
      //转为对象
      Row(cid,cname) //apply()
    })
    val schema=StructType(List(
        StructField("cid",IntegerType,true),
        StructField("cname",StringType,true)
    ))
    val classDataFrame=spark.createDataFrame(classesRDD,schema)

    val studentRDD=studentsLinesDataser.map(line=>{
      val fields=line.split(",")
      val id=fields(0).toInt
      val name=fields(1).toString
      val age=fields(2).toInt
      val height=fields(3).toDouble
      val cid=fields(4).toInt
      Row(id,name,age,height,cid)
    })
    val schema2=StructType(List(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("height",DoubleType,true),
      StructField("classid",IntegerType,true)
    ))
    val studentDataFrame=spark.createDataFrame(studentRDD,schema2)

    import  spark.implicits._
    //2.引入dataFrame Api+DSL来完成联接查询
    val resultDataFrame=classDataFrame.joinWith(studentDataFrame,$"cid"===$"classid","left_outer")
    resultDataFrame.show()
  }
}
