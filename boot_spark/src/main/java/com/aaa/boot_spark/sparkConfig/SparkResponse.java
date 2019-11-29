package com.aaa.boot_spark.sparkConfig;

import lombok.Data;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/29
 */
@Data
public class SparkResponse {
    private String action;
    private String message;
    private String serverSparkVersion;
    private String submissionId;
    private String success;
}
