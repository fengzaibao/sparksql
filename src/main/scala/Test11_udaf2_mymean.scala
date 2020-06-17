
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

object Test12_udaf2_mymean {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()
    //注册函数
    import  spark.implicits._
    //生成数据
    val range=spark.range(1,11)
    val rangedataset=range.map(x=>{
      x.toDouble
    })

    //调用自定义函数，完成计算
//    val studnetdataset=spark.read.textFile("data/students.txt")
//    val student=studnetdataset.map(line=>{
//      val fields=line.split(",")
//      val height=fields(3).toDouble
//      Students(height)
//    })

    rangedataset.select(GeoMean.toColumn.name("geoMean")).show()
    spark.stop()
  }
}

//初始化buffer  //1L代表Long
object GeoMean extends Aggregator[Double,MiddleResult,Double]{
  override def zero: MiddleResult = {
      MiddleResult(1.0,0L)
  }

  //在一个分区上时，当传入一条新数据，由spark框架自动回调方法，注入（DI） 两个参数
  //buffer缓冲区
  //input 一条数据
  override def reduce(b: MiddleResult, a: Double): MiddleResult = {
    b.product=b.product*a
    b.count=b.count+1
    b
  }

  override def merge(b1: MiddleResult, b2: MiddleResult): MiddleResult = {
    b1.product=b1.product*b2.product
    b1.count=b1.count+b2.count
    b1
  }

  //最终结果
  override def finish(reduction: MiddleResult): Double = {
      math.pow(reduction.product,1.toDouble/reduction.count)
  }

  override def bufferEncoder: Encoder[MiddleResult] = Encoders.product

  override def outputEncoder: Encoder[Double] =Encoders.scalaDouble
}
//中间的结果buffer
case class MiddleResult(var product:Double,var count:Long)
case class Students(height:Double)