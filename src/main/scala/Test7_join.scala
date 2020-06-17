import org.apache.spark.sql.{Dataset, SparkSession}

object Test7_join {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    import  spark.implicits._
    val classesLinesDataset:Dataset[String]=spark.read.textFile("data/classes.txt")
    val studentsLinesDataset:Dataset[String]=spark.read.textFile("data/students.txt")
    val classesDataset=classesLinesDataset.map(line=>{
      val fields=line.split(" ")
      val cid=fields(0).toInt
      val cname=fields(1)
      //转成对象
      Classes(cid,cname) //apply()
    })
    val studentsDataset= studentsLinesDataset.map(line=>{
      val fields=line.split(",")
      val id=fields(0).toInt
      val name=fields(1)
      val age=fields(2).toInt
      val height=fields(3).toDouble
      val cid=fields(4).toInt
      Student(id,name,age,height,cid)
    })
    val studentsDataframe=studentsDataset.toDF()
    val classDataframe=classesDataset.toDF()

    //sql方案
    classDataframe.createTempView("v_classes")
    studentsDataframe.createTempView("v_students")
    val result=spark.sql("select id,name,age,height,v_classes.cid,cname from v_students inner join v_classes on v_students.cid=v_classes.cid")
    result.show()
    spark.stop()
  }
}
case class Classes(cid:Integer,cname:String)
case class Student(id:Integer,name:String,age:Integer,height:Double,cid:Integer)

