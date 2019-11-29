package com.aaa.sparkDataSources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/27
 */
public class DataSourceAppV2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();
//        text(spark);
        json(spark);
        spark.stop();
    }

    /**
     * JSON
     * @param spark
     */
    private static void json(SparkSession spark) {
//        Dataset<Row> result = spark.read().json("wc/srcdata/people.json");

        //     SaveMode.Append
        //如果才能支持使用text方式输出多列的值呢？
//        result.filter("age > 20").select("name").write().mode("overwrite").text("out") ;//
        // 对于复杂的json操作
        Dataset<Row> jsonDF2 = spark.read().json("wc/srcdata/people2.json");
        jsonDF2.select(col("name"), col("age"), col("info.adr").as("adr_new"))

                .write().mode("overwrite").json("out");
    }

    /**
     * text 文本
     * @param spark
     */
    private static void text(SparkSession spark) {

        Dataset<Row> result = spark.read().text("wc/srcdata/people.txt");

        //     SaveMode.Append
        //如果才能支持使用text方式输出多列的值呢？
        result.write().mode("overwrite").text("out") ;//

        // 回忆一下：Hadoop中MapReduce的输出，第一次OK，第二次就会报输出目录已存在
    }
}
