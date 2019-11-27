package com.aaa.bigdata.hadoop.sparkDataSources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/27
 */
public class JDBCApi {
    public static void main(String[] args) {


        // 注意：JDBC的加载和保存可以通过load/save或JDBC方法来实现
        //从JDBC源加载数据
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306")
                .option("dbtable", "test.a")
                .option("user", "root")
                .option("password", "123456")
                .load();

        //将数据保存到JDBC源
       /* jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306")
                .option("dbtable", "test.test_new")
                .option("user", "root")
                .option("password", "123456")
                .save();*/

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:mysql://localhost:3306", "test.test", connectionProperties);

//        jdbcDF2.write()
//                .jdbc("jdbc:mysql://localhost:3306", "test.test_new3", connectionProperties);

            // 指定写入时创建表列数据类型
        jdbcDF.write()
                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
                .jdbc("jdbc:mysql://localhost:3306", "test.test_new2", connectionProperties);
    }
}
