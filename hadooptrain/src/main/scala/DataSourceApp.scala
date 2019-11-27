import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DataSourceApp {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("wc").getOrCreate()

     text(spark)
    // json(spark)
    // common(spark)
    // parquet(spark)

    // convert(spark)

    // jdbc(spark)
//    jdbc2(spark)
    spark.stop()
  }

  // 代码打包，提交到YARN或者Standalone集群上去，注意driver的使用
  def jdbc2(spark:SparkSession): Unit = {
    import spark.implicits._

    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table")
    val sinkTable = config.getString("db.default.sink.table")

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val jdbcDF: DataFrame = spark.read.jdbc(url, s"$database.$table", connectionProperties)

    jdbcDF.filter($"cnt" > 100).show() //.write.jdbc(url, s"$database.$sinkTable", connectionProperties)
  }
  /**
    * 有些数据是在MySQL，如果使用Spark处理，肯定需要通过Spark读取出来MySQL的数据
    * 数据源是text/json，通过Spark处理完之后，我们要将统计结果写入到MySQL
    */
  def jdbc(spark:SparkSession): Unit = {
    import spark.implicits._

//    val jdbcDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://hadoop000:3306")
//      .option("dbtable", "spark.browser_stat")
//      .option("user", "root")
//      .option("password", "root")
//      .load()
//
//    jdbcDF.filter($"cnt" > 100).show(100)

    // 死去活来法

    val url = "jdbc:mysql://hadoop000:3306"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    val jdbcDF: DataFrame = spark.read
      .jdbc(url, "spark.browser_stat", connectionProperties)

    jdbcDF.filter($"cnt" > 100)
      .write.jdbc(url, "spark.browser_stat_2", connectionProperties)
  }

  // 存储类型转换：JSON==>Parquet
  def convert(spark:SparkSession): Unit = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.format("json").load("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/people.json")
//    jsonDF.show()

    jsonDF.filter("age>20").write.format("parquet").mode(SaveMode.Overwrite).save("out")

    spark.read.parquet("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/out").show()

  }

  // Parquet数据源
  def parquet(spark:SparkSession): Unit = {
    import spark.implicits._

    val parquetDF: DataFrame = spark.read.parquet("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/users.parquet")
//    parquetDF.printSchema()
//    parquetDF.show()

//    parquetDF.select("name","favorite_numbers")
//      .write.mode("overwrite")
//        .option("compression","none")
//      .parquet("out")

    spark.read.parquet("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/out").show()
  }

  // 标准API写法
  def common(spark:SparkSession): Unit = {
    import spark.implicits._

    // 源码面前 了无秘密
//    val textDF: DataFrame = spark.read.format("text").load("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/people.txt")
    val jsonDF: DataFrame = spark.read.format("json").load("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/people.json")
//
//    textDF.show()
//    println("~~~~~~~~")
//    jsonDF.show()

    jsonDF.write.format("json").mode("overwrite").save("out")

  }

    // JSON
  def json(spark:SparkSession): Unit = {
    import spark.implicits._
    val jsonDF: DataFrame = spark.read.json("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/people.json")

    //jsonDF.show()

    // TODO... 只要age>20的数据
    //jsonDF.filter("age > 20").select("name").write.mode(SaveMode.Overwrite).json("out")

    val jsonDF2: DataFrame = spark.read.json("wc/srcdata/people.json")
    jsonDF2.select($"name",$"age",$"info.work".as("work"), $"info.home".as("home")).write.mode("overwrite").json("out")
  }

  // 文本
  def text(spark:SparkSession): Unit = {

    import spark.implicits._
    val textDF: DataFrame = spark.read.text("wc/srcdata/people.txt")

    // textDF.show()
    val result: Dataset[(String)] = textDF.map(x => {
      val splits: Array[String] = x.getString(0).split(",")
      (splits(0).trim) //, splits(1).trim
    })

//     SaveMode.Append
    result.write.mode("overwrite").text("out") // 如果才能支持使用text方式输出多列的值呢？

    // 回忆一下：Hadoop中MapReduce的输出，第一次OK，第二次就会报输出目录已存在
  }
}
