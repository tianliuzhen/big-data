import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InteroperatingRDDs {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("wc").getOrCreate()
//    runInferSchema(spark)

    programmatically(spark)
    spark.stop()
    }
  /**
    * 第二种方式 自定义
    * @param spark
    */
 private def programmatically(spark: SparkSession): Unit ={
   import spark.implicits._
   val rdd: RDD[String] = spark.sparkContext.textFile("wc/srcdata/people2.txt")
   //step1
   val peopleRow: RDD[Row] = rdd.map(_.split(","))
     .map(x => Row(x(0), x(1).trim.toInt))
   //step2
   val struct=  StructType(
     Array(StructField("name",StringType,true),StructField("name",IntegerType,true))
   )
   //step3
  val peopleDF=  spark.createDataFrame(peopleRow,struct)

   peopleDF.show()

   spark.stop()

 }
  /**
    * 第一种方式 反射
    * 1、定义case class
    * 2、RDD map,map中每一行数据case class
    * @param spark
    */
  private def runInferSchema(spark: SparkSession) = {
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.textFile("wc/srcdata/people2.txt")
    // todo ..rdd  转 => dataFrame
    val peopleDF: DataFrame = rdd.map(_.split(","))
      .map(x => Person(x(0), x(1).trim.toInt))
      .toDF()
    //    peopleDF.show(false)
    //采用sql
    peopleDF.createOrReplaceTempView("person")
    val queryDF: DataFrame = spark.sql("select name,age from person")
    //按下标取
    queryDF.map(x => "Name:" + x(0)).show()
    //按字段名取
    queryDF.map(x => "Name:" + x.getAs[String]("name")).show()
  }

  case class Person(name: String, age: Long)
}
