package com.aaa.bigdata.hadoop.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

/**
 * description: java 操作 spark dataSet
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/26
 */
public class SparkDataSet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();


        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32L);

       // 为javabean创建编码器
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

        // 类编码器中提供了大多数常见类型的编码器
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect();
        // show [2, 3, 4]
        transformedDS.show();

        // 通过提供类，可以将数据帧转换为数据集。基于名称的映射
        String path = "wc/srcdata/people.txt";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);

        Dataset<Person> teenagerNamesByFieldDF = peopleDS.map(
                (MapFunction<Person, Person>) value -> {
                    if(value.getAge()>33){
                        return value;
                    }else{
                        return null;
                    }
                },
                personEncoder);

        teenagerNamesByFieldDF.show();
    }
}

