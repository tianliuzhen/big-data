package com.aaa.bigdata.hadoop.mapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * description: reduce 的任务处理类
 * Reducer 类的四个泛型中，前两个输入要与 Mapper 的输出相对应。输出需要联系具体情况自定义
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/22
 */
public class WCReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    //框架在 map 处理完之后，将所有的 kv 对缓存起来，进行分组，然后传递一个分组(<key,{values}>,例如：<"hello",{1,1,1,1}>),
    //调用此方法
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        long count = 0;
        for (LongWritable val : values) {
            count += val.get();
        }
        context.write(key,new LongWritable(count));
    }
}
