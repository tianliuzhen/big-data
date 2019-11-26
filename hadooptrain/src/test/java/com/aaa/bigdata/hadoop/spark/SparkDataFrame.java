package com.aaa.bigdata.hadoop.spark;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
/**
 * description: java 操作 spark dataFrame
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/26
 */
public class SparkDataFrame {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("wc/srcdata/people.txt");
        /**
         * 测试 dml
         */
        System.out.println("测试 dml: Untyped Dataset Operations (aka DataFrame Operations)");
        df.printSchema();
        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();
        // > 21
        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

        df.show();

        /**
         * 测试 整条sql
         */
        System.out.println("测试 sql:Running SQL Queries Programmatically");
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        /**
         * 测试 视图
         */
        System.out.println("测试 视图：Global Temporary View");


        //将DataFrame注册为全局临时视图
        df.createGlobalTempView("people");
        //全局临时视图与系统保留的数据库`global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        //全局临时视图是跨会话
        spark.newSession().sql("SELECT * FROM global_temp.people").show();

        spark.stop();







    }
}
