import DataSourceApp.text
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DataSourceAppV2 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("wc").getOrCreate()

//    text(spark)
    json(spark)
//    common(spark)
//    parquet(spark)
//    convert(spark)
    spark.stop()
  }

  // 存储类型转换：JSON==>Parquet
  def convert(spark:SparkSession): Unit = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.format("json").load("wc/srcdata/people.json")
    //    jsonDF.show()

    jsonDF.filter("age>20").write.format("parquet").mode(SaveMode.Overwrite).save("out")

    spark.read.parquet("out").show()

  }

  /**
    * Parquet数据源
    * @param spark
    */
  def parquet(spark:SparkSession): Unit = {
    import spark.implicits._

    val parquetDF: DataFrame = spark.read.parquet("wc/srcdata/users.parquet")
//        parquetDF.printSchema()
//        parquetDF.show()

        parquetDF.select("name","favorite_numbers")
          .write.mode("overwrite")
            .option("compression","none")
          .parquet("out")

    spark.read.parquet("out/").show()
  }

  /**
    * 标准API写法
    * @param spark
    */
  def common(spark:SparkSession): Unit = {
    import spark.implicits._

    // 源码面前 了无秘密
    //    val textDF: DataFrame = spark.read.format("text").load("wc/srcdata/people.txt")
    val jsonDF: DataFrame = spark.read.format("json").load("wc/srcdata/people.json")
    //
    //    textDF.show()
    //    println("~~~~~~~~")
    //    jsonDF.show()

    jsonDF.write.format("json").mode("overwrite").save("out")

  }


  /**
    * JSON
    * @param spark
    */
  def json(spark:SparkSession): Unit = {
    import spark.implicits._
//    val jsonDF: DataFrame = spark.read.json("wc/srcdata/people.json")

    //jsonDF.show()

    // TODO... 只要age>20的数据
//    jsonDF.filter("age > 20").select("name").write.mode(SaveMode.Overwrite).json("out")

    //对于复杂的json
    val jsonDF2: DataFrame = spark.read.json("wc/srcdata/people2.json")
    jsonDF2.select($"name",$"age",$"info.work".as("work"), $"info.adr".as("adr")).write.mode("overwrite").json("out")
  }

  /**
    * 文本
    * @param spark
    */
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
