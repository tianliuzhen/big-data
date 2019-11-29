package com.aaa.boot_spark.sparkConfig;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.spark_project.jetty.client.HttpClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/29
 */
public class test {
    public static void main(String[] args) {
        createJob();
    }
    public static String   createJob(){
         JobSubmitRequest jobSubmitRequest = new JobSubmitRequest();
        jobSubmitRequest.setAction(Action.CreateSubmissionRequest);
        List<String> appArgs = new ArrayList<>();
        appArgs.add("starttime");
        appArgs.add("endtime");
        appArgs.add("servicemaxnum");
        appArgs.add("key");
        jobSubmitRequest.setAppArgs(new ArrayList<>());
        jobSubmitRequest.setAppResource("/opt/data/"+"hadoop-train-1.0.jar");
        jobSubmitRequest.setClientSparkVersion("2.3.1");
        jobSubmitRequest.setMainClass("SparkWordCountApp");
        Map<String, String> environmentVariables = new HashMap<>();
        environmentVariables.put("SPARK_ENV_LOADED", "1");
        jobSubmitRequest.setEnvironmentVariables(environmentVariables);
        JobSubmitRequest.SparkProperties sparkProperties = new JobSubmitRequest.SparkProperties();
        sparkProperties.setJars("/opt/data/"+"hadoop-train-1.0.jar");
        sparkProperties.setAppName("SubmitScalaJobToSpark");
        sparkProperties.setOtherProperties("spark.submit.deployMode", "cluster");
        sparkProperties.setMaster("spark://"+"tian:7077");
        jobSubmitRequest.setSparkProperties(sparkProperties);
        CloseableHttpClient client = HttpClients.createDefault();
        final String url = "http://"+"47.98.253.2:8080"+"/v1/submissions/create";
        final HttpPost post = new HttpPost(url);
        post.setHeader(HTTP.CONTENT_TYPE, "application/json;charset=UTF-8");

        try {
            final String message = JSONObject.toJSONString(jobSubmitRequest);
            post.setEntity(new StringEntity(message.toString()));
            final String stringResponse = client.execute(post, new BasicResponseHandler());
            if (stringResponse != null) {
                SparkResponse response = JSON.parseObject(stringResponse,SparkResponse.class);
                return response.getSubmissionId();
            } else {
                return "FAILED";
            }
        } catch (Exception e) {
            System.out.println(e);
            return "FAILED";
        }
    }
}
