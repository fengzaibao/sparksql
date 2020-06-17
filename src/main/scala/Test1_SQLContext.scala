import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
 * spark 1.x创建dataframe方案
 * 利用SQLContext来编程（转化case class对象来创建DataFrame）
 * 1.先创建SparkContext，在创建SQLContext
 * 2.先创建RDD，对数据进行整理，然后关联case class，将非结构化数据转换为结构化数据
 * 3.显示调用toDF将RDD转为DataFrame
 * 4.注册临时表
 * 5.执行SQL（这种SQL都是Trasformation，lazy操作，不是action操作）
 * 6.执行Action
 */
object Test1_SQLContext {
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

      //转为结构化数据
      Person(id,name,age,height)
    })
    //引入隐式转换，将rdd转为DataFrame
    import sqlContext.implicits._
    val df=personRDD.toDF()
    df.show()

    //得到DataFrame后，有两种操作1.SQL 2.API
    df.registerTempTable("person")
    val resultDataFrame=sqlContext.sql("select * from person order by age desc,height desc,name asc")

    resultDataFrame.show()


    sc.stop()

  }
}
//case class Person(id:Integer,name:String,age:Integer,height:Double)