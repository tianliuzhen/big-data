import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkDataSet {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[2]").appName("wc").getOrCreate()
    import spark.implicits._

    val ds: Dataset[Person] = Seq(Person("pk",30)).toDS()
//    ds.show()

    val ds2 :Dataset[Int] = Seq(1,2,3).toDS()
    ds2.map(x => x+1).collect().foreach(println)
    // dataFrame 转 dataSet
    val peopleDf: DataFrame= spark.read.json("wc/srcdata/people.json")
    val peopleDf2: Dataset[Person]  =peopleDf.as[Person]
    peopleDf2.show()
    peopleDf.map(x => "name").show() //这里可能 会写错 ，所以dataset能编译时检查类型
    peopleDf2.map(x => x.name).show()
    spark.stop()
  }

  case class Person(name: String, age: Long)
}
