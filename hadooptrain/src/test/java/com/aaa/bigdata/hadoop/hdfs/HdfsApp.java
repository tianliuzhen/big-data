package com.aaa.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
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
public class HdfsApp {
    private final String  HDFS_PATH = "hdfs://47.98.253.2:8020";
    Configuration configuration=null;
    FileSystem fileSystem=null;

    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        configuration=new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");//让可以使用主机名传参数
        /**
         * 构造一个指定 HDFS 系统的客户端对象
         * 参数1 连接hfds的端口 8020 在  core-site.xml  中
         * 参数2 返回已经配置的文件系统的实现
         * 参数3 客户端的身份，用户名
         */
        fileSystem=    FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
    }
    /**
     * 测试创建目录
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfs-api/test4"));
    }

    /**
     * 查看HDFS内容
     * @throws IOException
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream in= fileSystem.open(new Path("/t.txt"));
        IOUtils.copyBytes(in,System.out,1024,true);
        System.out.println("end");
        in.close();
    }

    /**
     * 写入HDFS内容
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream out= fileSystem.create(new Path("/a.txt"));
        out.write("test_java_api".getBytes());
        out.flush();
        out.close();
    }

    @After
    public void tearDown(){
        configuration=null;
        fileSystem=null;
    }

}
