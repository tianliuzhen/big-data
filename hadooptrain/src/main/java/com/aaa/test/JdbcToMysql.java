package com.aaa.test;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.sql.*;

import java.util.*;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/12/3
 */
@Slf4j
public class JdbcToMysql {
    public static String url= "jdbc:mysql://rm-bp1p20iser0op442jvo.mysql.rds.aliyuncs.com:3306";
    public static String url2= "jdbc:mysql://172.18.1.244:3306";
    public static String url3= "jdbc:mysql://127.0.0.1:3306";
    public static void main(String[] args) {
        // 注意：JDBC的加载和保存可以通过load/save或JDBC方法来实现
        //从JDBC源加载数据
        System.out.println();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        Dataset<Row> jdbcDF=  getDBBy2(spark);
        log.info("加载完毕");
       jdbcDF.createOrReplaceTempView("tmp");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM  tmp  limit 10");

        Person person = new Person();
        person.setName("Andy");
        person.setAge(32L);
        // 为javabean创建编码器
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);



        javaBeanDS.select("title");
        sqlDF.show();
        log.info("step1 结束。。。。");
       sqlDF.write().mode("overwrite").text("out") ;
//        jdbcDF.show(200,true);

    }


    /**
     *  一、不指定查询条件
     */
   public static   Dataset<Row>  getDBBy1( SparkSession spark){

       Properties cp = getProperties2();
       Dataset<Row> jdbcDF = spark.read()
               .jdbc(url3, "test.a", cp);
               //.where(" time > 1575339009 ");

       return  jdbcDF;
   }

    /**
     *  二、指定查询条件
     */
    public  static Dataset<Row>  getDBBy2( SparkSession spark){

        Properties cp = getProperties();
        Dataset<Row> jdbcDF = spark.read()
                /**
                 *  在连接时之间用sql进行查询
                 */
                .jdbc(url, " (SELECT * FROM    hxwx.wxdata_article_analysis where time > 1575515140 ) tmp ", cp);

        return  jdbcDF;
    }

    /**
     *  三（分区）、指定数据库字段的范围
     *
     *  这种方式就是通过指定数据库中某个字段的范围，但是遗憾的是，这个字段必须是数字，来看看这个函数的函数原型：
     *  这个方法可以将iteblog表的数据分布到RDD的几个分区中，分区的数量由numPartitions参数决定，
     *  在理想情况下，每个分区处理相同数量的数据，我们在使用的时候不建议将这个值设置的比较大，因为这可能导致数据库挂掉！
     *  但是根据前面介绍，这个函数的缺点就是只能使用整形数据字段作为分区关键字。
     *    “numPartition” 在这里非常重要，它告诉 Spark 并行的执行多个查询，每个分区分配一个查询执行。
     * 　　这个函数在极端情况下，也就是设置将numPartitions设置为1，其含义和第一种方式一致。
     */

    public static  Dataset<Row>  getDBBy3( SparkSession spark){
        Properties cp = getProperties2();
        Dataset<Row> jdbcDF = spark.read()
                .jdbc(url2,
                        "(SELECT * FROM   wml_authorize.crm_member  ) as tmp",
                        "total_cost_num",
                        1L,
                        100000L,
                        1,
                        cp);

        return  jdbcDF;
    }

    /**
     * 四 （分区）、根据任意字段进行分区
     * 基于前面两种方法的限制，Spark还提供了根据任意字段进行分区的方法，函数原型如下：
     *
     * 注意：最后rdd的分区数量就等于predicates.length。
     * @return
     */

    public static  Dataset<Row>  getDBBy4( SparkSession spark){
        Properties cp = getProperties();
       // array 里面写的是 条件  例如
        String[] array={"reportDate <= '2014-12-31'", "reportDate > '2014-12-31' and reportDate <= '2015-12-31'"};
        Dataset<Row> jdbcDF = spark.read()
                .jdbc(url,
                        "hxwx.wxdata_article_analysis",
                        array ,
                        cp);

        return  jdbcDF;
    }

    /**
     * 五、通过load获取
     * options函数支持url、driver、dbtable、partitionColumn、lowerBound、upperBound以及numPartitions选项，
     * 细心的同学肯定发现这个和方法二的参数一致。是的，其内部实现原理部分和方法二大体一致。同时load方法还支持json、orc等数据源的读取
     * @param spark
     * @return
     */
    public static  Dataset<Row>  getDBBy5( SparkSession spark){
        Properties cp = getProperties();
        // array 里面写的是 条件  例如
        String[] array={"reportDate <= '2014-12-31'", "reportDate > '2014-12-31' and reportDate <= '2015-12-31'"};
        Map<String, String> map=new HashMap();
        map.put("url","");
        map.put("driver","");
        map.put("dbtable","");
        map.put("partitionColumn","");
        map.put("lowerBound","");
        map.put("upperBound","");
        map.put("numPartitions","");
        Dataset<Row> jdbcDF = spark.read().format("jdbc").options(map).load();

        return  jdbcDF;
    }


    public static Properties getProperties() {
        Properties cp = new Properties();
        cp.put("user", "xiabuy");
        cp.put("password", "hxkj135.");
        cp.put("driver", "com.mysql.jdbc.Driver");
        return cp;
    }
    public static Properties getProperties2() {
        Properties cp = new Properties();
        cp.put("user", "root");
        cp.put("password", "haoxia");
        cp.put("driver", "com.mysql.jdbc.Driver");
        return cp;
    }
    /**
     *   使用spark-sql从db中读取数据, 处理后再回写到db
     * */
    public void db2db(SparkSession spark) {
        String url = "jdbc:mysql://10.93.84.53:3306/big_data?characterEncoding=UTF-8";
        String fromTable = "accounts";
        String toTable = "accountsPart";
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "1234");
        Dataset rows = spark.read().jdbc(url, fromTable, props).where("count < 1000");
        rows.write().mode(SaveMode.Append).jdbc(url, toTable, props);
    }



}
