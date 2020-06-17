import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object Test10_udaf_mymean {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()
    //创建函数并主持
    val myMean=new MyMean()
    spark.udf.register("myMean",myMean)
    //生成数据Dataset[Long]
    val range=spark.range(1,11)
    println("生成的数据:"+range.collect().toList)
    //1.SQL方案
    range.createTempView("v_range")
    spark.sql("select myMean(id) as mm,avg(id) as sysavg from v_range").show()

    //2.DSL方案
    import spark.implicits._
    //range.selectExpr($"myMean(id)".as("mm").toString()).show()
    spark.stop()
  }
}

/**
 * 自定义一个UDAF函数
 * 功能:求简单几何平均数
 */
class MyMean extends UserDefinedAggregateFunction{
  //聚合函数的输入数据结构  select MyMean(height) from 表名

  override def inputSchema: StructType ={
    StructType(List(StructField("value",DoubleType)))
  }
//运算时产生的中间结果的缓存区数据结构
  override def bufferSchema: StructType ={
    StructType(List(StructField("product",DoubleType),StructField("counts",LongType)))
  }
//返回值数据结构
  override def dataType: DataType ={
    DoubleType
  }

  //聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
  override def deterministic: Boolean = true

  //指定初始值 缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=1.0
    buffer(1)=0L
  }

  /**
   * 在一个分区上时，当传入一条新数据，由spark框架自动回调方法，注入（DI） 两个参数
   * @param buffer 存缓冲区
   * @param input 一条数据
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getDouble(0)*input.getDouble(0)
    buffer(1)=buffer.getLong(1)+1
  }

  //不同分区结果做全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getDouble(0)*buffer2.getDouble(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  //最终结果运算
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(0),1.toDouble/buffer.getLong(1))  //pow  平方
  }
}
