package com.aaa.boot_spark.spark;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/29
 */
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.CreateSubmissionResponse;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.apache.spark.deploy.rest.SubmissionStatusResponse;
import scala.collection.immutable.HashMap;

public class test {
    public static void main(String[] args) {
        String id = test.submit();
        boolean flag;
        while (true){
            flag = test.monitory(id);
            if (flag) {
                break;
            }
        }
        System.out.println("spark执行完成");
    }

    public static String submit() {
        String appResource = "/opt/data/hadoop-train-1.0.jar";
        String mainClass = "SelectLog";
        String[] args = {

        };  //spark程序需要的参数

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("spark://47.98.253.2:6066");
        sparkConf.set("spark.submit.deployMode", "cluster");
        sparkConf.set("spark.jars", appResource);
        sparkConf.set("spark.driver.supervise", "false");
        sparkConf.setAppName("queryLog"+ System.currentTimeMillis());

        CreateSubmissionResponse response = null;

        try {
            response = (CreateSubmissionResponse)
                    RestSubmissionClient.run(appResource, mainClass, args, sparkConf, new HashMap<String,String>());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response.submissionId();
    }

    private static RestSubmissionClient client = new RestSubmissionClient("spark://47.98.253.2:6066");

    public static boolean monitory(String appId){
        SubmissionStatusResponse response = null;
        boolean finished =false;
        try {
            response = (SubmissionStatusResponse) client.requestSubmissionStatus(appId, true);
            if("FINISHED" .equals(response.driverState()) || "ERROR".equals(response.driverState())){
                finished = true;
            }
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return finished;
    }
}
