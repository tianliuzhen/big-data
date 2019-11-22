package com.aaa.bigdata.hadoop.mapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description:
 * 此类用来描述一个特定的作业
 *  * 例：1.该作业使用哪个类作为逻辑处理中的 map，哪个作为 reduce
 *  *       2.指定该作业要处理的数据所在的路径
 *  *       3.指定该作业输出的结果放到哪个路径
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/22
 */
public class WCRunner {
    public static void main(String[] args) throws Exception {
        //1.获取 Job 对象:使用 Job 静态的 getInstance() 方法，传入 Configuration 对象
       Configuration conf=new Configuration() ;
       Job wcJob=Job.getInstance(conf,"word count");
        //2.设置整个 Job 所用的类的 jar 包：使用 Job 的 setJarByClass(),一般传入  当前类.class
        wcJob.setJarByClass(WCRunner.class);

        //3.设置本 Job 使用的 mapper 和 reducer 的类
        wcJob.setMapperClass(WCMapper.class);
        wcJob.setReducerClass(WCReducer.class);

        //4.指定 reducer 输出数据的 kv 类型  注：若 mapper 和 reducer 的输出数据的 kv 类型一致，可以用如下两行代码设置
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);

        //5.指定 mapper 输出数据的 kv 类型
        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);

        //6.指定原始的输入数据存放路径:使用 FileInputFormat 的 setInputPaths() 方法
        FileInputFormat.setInputPaths(wcJob,new Path("wc/srcdata/text.txt"));


        FileSystem fs = FileSystem.get(conf);
        boolean b= fs.exists(new Path("wc/ouput/"));
        if(b){
           fs.delete(new Path("wc/ouput/"));
        }
        //7.指定处理结果的存放路径：使用 FileOutputFormat 的 setOutputFormat() 方法
        FileOutputFormat.setOutputPath(wcJob,new Path("wc/ouput/"));

        //8.将 Job 提交给集群运行，参数为 true 表示显示运行状态
        System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
    }
}
