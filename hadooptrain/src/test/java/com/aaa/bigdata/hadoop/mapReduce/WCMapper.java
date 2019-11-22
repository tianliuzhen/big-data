package com.aaa.bigdata.hadoop.mapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description: 创建 map 的任务处理类：WCMapper
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/22
 */
public class WCMapper  extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        //书写具体的业务逻辑，业务要处理的数据已经被框架传递进来，就是方法的参数中的 key 和 value
        //key 是这一行数据的起始偏移量，value 是这一行的文本内容

        //1.将 Text 类型的一行的内容转为 String 类型
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
