import org.apache.spark.sql.SparkSession
import utils.{IpRule, YcUtil}

object Test9_accessAnalysis_broadcast {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import  spark.implicits._
    val ipLinesDataset=spark.read.textFile("data/ip.txt") //Dataset只有一列，有多行
    val ipDataset=ipLinesDataset.map(line=>{
      val fields=line.split("\\|")
      val startNum=fields(2).toLong
      val endNum=fields(3).toLong
      val province=fields(6)
      IpRule(startNum,endNum,province) //注意：Dataset(样例类对象) -> toDF()  对比之前:RDD[Row]->StructType转换成DataFrame
    })
    val ipDataFrame=ipDataset.toDF("startNum","endNum","province")
    //发布成广播变量
    val ipRulesArray=ipDataFrame.collect()
    val broadcastRef=spark.sparkContext.broadcast(ipRulesArray)
    val accessDataset=spark.read.textFile("data/access.log")
    val accessIpLongDataFrame=accessDataset.map(line=>{
      val fields=line.split("\\|")
      val ip=fields(1) //字符串
      val ipNum=YcUtil.ip2Long(ip)
      ipNum
    }).toDF("ipNum")

    //方案一：SQL方案
    //1.构建临时表或视图
    accessIpLongDataFrame.createTempView("v_accessIpLong") //条件

    //自定义函数:sql自定义函数
    //hive自定义函数 UDF 1V1 to_data     UDA:n V 1  sum,avg      UDTF:1Vn  pivot_table
    spark.udf.register("ip2province",(ipNum:Long)=>{
      //通过广播变量取出 ipRulesArray
      val ipRulesArray=broadcastRef.value
      var provice="unkown"
      var index=YcUtil.binarySearch(ipRulesArray,ipNum)
      if(index != -1){
          provice=ipRulesArray(index).getAs("province")
      }
      provice
    })
    val resultDataFrame=spark.sql("select ip2province(ipNum)province,count(*) cn from v_accessIpLong group by province")

    resultDataFrame.show()
    spark.stop()
  }
}
