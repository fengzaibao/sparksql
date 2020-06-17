import org.apache.spark.sql.SparkSession
import utils.YcUtil

object Test8_accessAnalysis {
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
      (startNum,endNum,province)
    })
    val ipDataFrame=ipDataset.toDF("startNum","endNum","province")

    val accessDataset=spark.read.textFile("data/access.log")
    val accessIpLongDataFrame=accessDataset.map(line=>{
      val fields=line.split("\\|")
      val ip=fields(1) //字符串
      val ipNum=YcUtil.ip2Long(ip)
      ipNum
    }).toDF("ipNum")

    //方案一:SQL方案
    //1.临时表或视图
    ipDataFrame.createTempView("v_ip")
    accessIpLongDataFrame.createTempView("v_accessIpLong") //条件
   // val resultDataFrame=spark.sql("select province,count(*) cn from v_accessIpLong inner join v_ip on (ipNum>=startNum and ipNum<=endNum) group by province")

    //方案二:API+DSL
    val resultDataFrame=ipDataFrame.join(accessIpLongDataFrame,$"ipNum">=$"startNum" and $"ipNum"<=$"endNum").groupBy("province").count()
     resultDataFrame.show()
    spark.stop()

  }
}
