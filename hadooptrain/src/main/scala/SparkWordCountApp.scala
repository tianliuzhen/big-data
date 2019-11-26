import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountApp {
  def main(args: Array[String]): Unit = {
    val s=new SparkConf().setMaster("local").setAppName("test");
    val sc =new SparkContext(s)
    val rdd =sc.textFile("wc/srcdata/text.txt")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
