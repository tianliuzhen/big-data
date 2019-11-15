package com.aaa.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/15
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    private static Configuration configuration = new Configuration();

    static {
        configuration.set("fs.defaultFS","hdfs://47.98.253.2:8020");
        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("hadoop.home.dir","E:/hadoop2.6");
//        configuration.set("dfs.client.use.datanode.hostname", "true");
    }

    public static void main( String[] args ) throws IOException {
//        createDir();
        //list();
        uploadFile();
        //readContent();
//        deleteDir();
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    private static void createDir() throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        fs.mkdirs(new Path("/hdfs2"));
        fs.close();
        System.out.println("创建成功");
    }

    /**
     * 上传文件（将文件拷贝到HDFS上）
     * @throws IOException
     */
    private static void uploadFile() throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Path srcPath = new Path("E:/1.txt");
        Path targetPath = new Path("/");
        fs.copyFromLocalFile(srcPath,targetPath);
        fs.close();
        System.out.println("上传成功");
    }

    /**
     * 展示hdfs文件或目录
     * @throws IOException
     */
    private static void list() throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatus:fileStatuses){
            String type = fileStatus.isDirectory() ? "目录":"文件";
            String name = fileStatus.getPath().getName();
            System.out.println(type + "---" + name);
        }
        fs.close();
    }

    /**
     * 读取文件内容
     * @throws IOException
     */
    private static void readContent() throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream stream = fs.open(new Path("/input/input2.txt"));
        IOUtils.copyBytes(stream,System.out,1024,true);
        IOUtils.closeStream(stream);
        fs.close();
    }

    /**
     * 删除目录
     * @throws IOException
     */
    private static void deleteDir() throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path("/hdfs2"),true);
        fs.close();
        System.out.println("删除成功");
    }
}
