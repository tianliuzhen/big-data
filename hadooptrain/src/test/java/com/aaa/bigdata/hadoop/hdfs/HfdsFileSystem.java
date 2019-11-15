package com.aaa.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/15
 */
public class HfdsFileSystem {


    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {


        Configuration configuration=new Configuration();
        /**
         * 参数1 连接hfds的端口 8020 在  core-site.xml  中
         * 参数2 返回已经配置的文件系统的实现
         * 参数3 连接用户
         */
        FileSystem fileSystem=    FileSystem.get(new URI("hdfs://47.98.253.2:8020"),configuration,"root");
        Path path=new Path("/hdfs-api/test");
        boolean b= fileSystem.mkdirs(path);
        System.out.println(b);
    }
    public void test(){

    }
}
