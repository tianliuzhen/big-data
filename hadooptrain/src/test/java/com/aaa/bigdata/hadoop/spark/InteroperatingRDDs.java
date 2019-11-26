package com.aaa.bigdata.hadoop.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * description: rdd 与 dataFrame和dataSet 互转
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/26
 */
public class InteroperatingRDDs {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();
//        reflection(spark);
        programmatically(spark);
    }

    /**
     * 2、以编程方式指定架构
     * @param spark
     */
    private static void programmatically(SparkSession spark) {
// Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("wc/srcdata/people2.txt", 1)
                .toJavaRDD();

// 架构编码为字符串
        String schemaString = "name age";

//基于架构字符串生成架构
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

// 将RDD（人员）的记录转换为行
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

// 将架构应用于RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

// 使用数据框创建临时视图
        peopleDataFrame.createOrReplaceTempView("people");

// SQL可以在使用DataFrames创建的临时视图上运行
        Dataset<Row> results = spark.sql("SELECT name FROM people");

//SQL查询的结果是数据帧，支持所有正常的RDD操作
//结果中行的列可以通过字段索引或字段名访问
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }

    /**
     * 1、反射方法
     * @param spark
     */
    private static void reflection(SparkSession spark) {
        //从文本文件创建Person对象的RDD
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("wc/srcdata/people2.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Long.parseLong(parts[1].trim()));
                    return person;
                });

//将模式应用于JavaBeans的RDD以获取数据帧
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

//将数据帧注册为临时视图
        peopleDF.createOrReplaceTempView("people");

//SQL语句可以使用spark提供的SQL方法运行
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

//结果中行的列可以通过字段索引访问  （按下标取值）
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();

// 按字段取值
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
        spark.stop();
    }
}
