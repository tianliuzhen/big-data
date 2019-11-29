package com.aaa.boot_spark.web;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/29
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.deploy.SparkSubmit;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.Date;


@Controller
@Slf4j
public class SparkJobController {


    @RequestMapping(value="/job/executeJob",method=RequestMethod.GET)
    @ResponseBody
    RetMsg executeSparkJob(@RequestParam("jarId") String jarId,@RequestParam("sparkUri") String sparkUri) {
        RetMsg ret = new RetMsg();
        StringBuffer msg = new StringBuffer(jarId+":"+sparkUri);
        ret.setMsg(msg.toString());
        SparkJobLog jobLog = new SparkJobLog();
        jobLog.setExecTime(new Date());
        String[] arg0=new String[]{
                "/opt/data/"+"hadoop-train-1.0.jar",
                "--master","spark://tian:7077",
                "--name","web polling",
                "--executor-memory","1G"
        };
        log.info("提交作业...");
        try {
            SparkSubmit.main(arg0);
        } catch (Exception e) {
            log.info("出错了！", e);
            ret.setCode(1L);
            ret.setMsg(e.getMessage());
            msg.append(e.getMessage());
        }
        jobLog.setMsg(msg.toString());
        return ret;
    }
}
