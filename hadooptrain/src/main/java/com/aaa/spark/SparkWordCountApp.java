package com.aaa.spark;

import com.aaa.common.DeleteDirectory;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/28
 */
public class SparkWordCountApp {
    public static void main(String[] args) {
        wordCount();
    }

    private static JavaSparkContext  getSC() {
        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCountApp").setMaster("local");
        // JavaSparkContext   =  SparkContext
        JavaSparkContext sc = new JavaSparkContext (sparkConf);
        return  sc;
    }
    public static void wordCount(){

        // 制作数据集：
        List data = Arrays.asList("Google Bye GoodBye Hadoop code", "Java code Bye");
        // 将数据转化为RDD
        JavaSparkContext sc = getSC();
        JavaRDD lines = sc.parallelize(data);
        // flatMap 转化逻辑：一行行转化为 "Google", "Bye"...
        JavaRDD words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator call(String lines) throws Exception {
                return Arrays.asList(lines.split(",")).iterator();
            }
        });
        //map 生成 ("Google", 1) 的key-value对
        JavaPairRDD word = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2 call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });
        // reduceByKey 根据 key 进行合并
        JavaPairRDD wordCnt = word.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 输出
        wordCnt.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> o) throws Exception {
                System.out.println(o._1 + ":" + o._2);
            }
        });
        DeleteDirectory.delZSPic("wc/srcdata/peopleOutNew.txt");
        wordCnt.saveAsTextFile("wc/srcdata/peopleOutNew.txt");
        sc.stop();
    }
}
