import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDataFrame {
 var a=1;

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("wc").getOrCreate()
   import spark.implicits._
   //    若是指定文件路径为 "file://…",则读取的是本地目录
    val people: DataFrame= spark.read.json("wc/srcdata/people.txt")
   // 查看df的内部结构：列名、列的类型、是否为空
    people.printSchema()
   //  TODO... select age,name from people group by age  查询某列
    //   people.select("name").show()
   //隐式转换

     //   people.select($"name").show()
//   people.filter($"age">21).show()
    //   people.filter("age>21").show()

   //分组查询
   // TODO... select age,count(1) from people group by age
//   people.groupBy("age").count().show()
   // TODO... select name,age+10 from people
//   people.select($"name",($"age"+10).as("new_age")).show()
   // TODO ... 使用纯sql
  /* people.createOrReplaceTempView("people")
   spark.sql("select * from people where age=99").show()*/

   //显示全部
//    people.show()

   /**
     *  DataFarme中前N条取值方式
     */

   val zips :DataFrame = spark.read.json("wc/srcdata/zips.json")
   zips.printSchema()

   /**
     * 1、字段 默认展示 字段 20个 ，超过默认补 ...
     * 2、默认显示前20条
     */
//   zips.show(10,false)

//   zips.head(3).foreach(println)
//   zips.first()
//   zips.take(2)

  val count :Long=  zips.count()
//   println(s"总共多少条数据$count")
   println("总共多少条数据"+count)
   zips.select($"_id".as("new_id").as("id"),$"city").filter(zips.col("pop")>40000).show(10,false)
//   zips.filter(zips.col("pop")>40000).withColumnRenamed("_id","new_id").show(10,false)

   import org.apache.spark.sql.functions._
   //统计加州pop最多的城市的名称
//   zips.select("_id","city","pop","state").filter(zips.col("state") === "CA").orderBy(desc("pop"))show(10,false)

   // sql 方式
   zips.createOrReplaceTempView("zips")
   spark.sql(" select * from  zips where state = 'CA' order by pop desc limit 10 ").show()


   spark.stop()

  }
}
