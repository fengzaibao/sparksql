import java.util.Properties

import org.apache.spark.sql.SparkSession

object Test13_jdbc {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //JSON/CSV/jdbc
    val resultDF=spark.read.format("jdbc").options(Map("url"->"jdbc:mysql://localhost:3306/bigdata","driver"->"com.mysql.jdbc.Driver","user"->"root","password"->"a","dbtable"->"ipresult"))
      .load()

    //表结构
    ;println("1.表结构")
    resultDF.printSchema()
    println("2.输出表内容(top 20)")
    resultDF.show()
    //对表中内容进行操作的话，有两种方法
    //1.sql
    //2.api
    println("3.以lambda（DSL）过滤")
    val filtered=resultDF.filter($"nums">1000)
    filtered.show()
    println("4.以where函数加入DSL过滤")
    val whered=resultDF.where($"nums">1000)
    whered.show()
    println("5.select函数指定返回的列(可以指定别名，进行运算)")
    val selected=resultDF.select($"id",$"province",$"nums"*10 as "changednums")

    //Specifies the behavior when data or table already exists. Options include:
    // 覆盖- overwrite: overwrite the existing data.
    // 追加- append: append the data.
    // 不操作- ignore: ignore the operation (i.e. no-op).
    // 报错- error: default option, throw an exception at runtime.
    //把结果保存到数据库
    println("6.结果保存到数据库")
    val props=new Properties()
    props.put("user","root")
    props.put("password","a")
    selected.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata","result",props)
    println("保存到mysql成功")

    println("7.保存到txt中")
    //异常 Text data source supports only a single column, and you have 3 columns.;
    //存成文本，只允许一个列，但是这里有三个列
    //selected.map(lines=>{
    //  lines(0)+line(1)
    //})
    selected.toJavaRDD.saveAsTextFile("data/txtResult")
    //selected.write.mode("append").text("data/txtResult") //hadoop:分区有多个，对应的输出文件就有多个

    println("8.保存到json")
    selected.write.mode("append").json("data/jsonResult")

    println("9.保存到csv")
    selected.write.mode("append").json("data/csvResult")

    println("10.保存到parquet")
    selected.write.mode("append").parquet("data/parquetResult")

    spark.stop()
  }
}
