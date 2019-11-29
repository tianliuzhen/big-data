package com.aaa.boot_spark.web;

import lombok.Data;

import java.util.Date;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/29
 */
@Data
public class SparkJobLog {
    private Date execTime;
    private String msg;
}
