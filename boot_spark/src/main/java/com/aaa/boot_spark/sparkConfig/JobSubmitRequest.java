package com.aaa.boot_spark.sparkConfig;

/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/29
 */

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
class JobSubmitRequest {

    private String action;

    private String appResource;

    private List<String> appArgs;

    private String clientSparkVersion;

    private String mainClass;

    private Map<String, String> environmentVariables;

    private SparkProperties sparkProperties;

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    static class SparkProperties {

        @JsonProperty(value = "spark.jars")
        private String jars;

        @JsonProperty(value = "spark.app.name")
        private String appName;

        @JsonProperty(value = "spark.master")
        private String master;

        private Map<String, String> otherProperties = new HashMap<>();
        public void setOtherProperties(Map<String, String> otherProperties) {
            this.otherProperties = otherProperties;
        }

        void setOtherProperties(String key, String value) {
            this.otherProperties.put(key,value);
        }

    }


}


