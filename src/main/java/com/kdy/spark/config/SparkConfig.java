package com.kdy.spark.config;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import scala.Serializable;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/4/7 0:11
 */

@Data
@Configuration
public class SparkConfig  {


    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master.uri}")
    private String sparkMasterUri;
    @Value("${spark.driver.memory}")
    private String sparkDriverMemory;
    @Value("${spark.worker.memory}")
    private String sparkWorkerMemory;
    @Value("${spark.executor.memory}")
    private String sparkExecutorMemory;
    @Value("${spark.executor.cores}")
    private String sparkExecutorCores;
    @Value("${spark.num.executors}")
    private String sparkExecutorsNum;
    @Value("${spark.network.timeout}")
    private String networkTimeout;
    @Value("${spark.executor.heartbeatInterval}")
    private String heartbeatIntervalTime;
    @Value("${spark.driver.maxResultSize}")
    private String maxResultSize;
    @Value("${spark.rpc.message.maxSize}")
    private String sparkRpcMessageMaxSize;
    @Value("${spark.storage.blockManagerSlaveTimeoutMs}")
    private String blockManagerSlaveTimeoutMs;

    @Value("${spark.jars}")
    private String jars;

//    @Value("${spark.jars}")
//    private String jars;


//    配置类  配置信息参考 https://www.cnblogs.com/sandea/p/11911509.html

    @Bean
    @ConditionalOnMissingBean(SparkConf.class)    //保证bean实例只有一个
    public SparkConf sparkConf()  {
        /**
         * appName：应用名称
         * sparkMasterUri：   spark://ip:7077          local
         * jars: 需要提交的jar包的位置
         * maxResultSize: 每个Spark action(如collect)所有分区的序列化结果的总大小限制。设置的值应该不小于1m，0代表没有限制。如果总大小超过这个限制，程序将会终止。大的限制值可能导致driver出现内存溢出错误（依赖于spark.driver.memory和JVM中对象的内存消耗）。
         *heartbeatInterval: executor 向 the driver 汇报心跳的时间间隔，单位毫秒
         * spark.rpc.message.maxSize : Spark节点间传输的数据过大，超过系统默认的128M，因此需要提高
         */
        SparkConf sparkConf=new SparkConf();
        if(sparkMasterUri.equals("local")){
                     sparkConf
                    .setAppName(appName)
                    .setMaster(sparkMasterUri)
//                .setJars(new String[]{jars})
                    .set("spark.driver.memory",sparkDriverMemory)
                    .set("spark.driver.maxResultSize",maxResultSize)
                    .set("spark.worker.memory",sparkWorkerMemory)
                    .set("spark.executor.memory",sparkExecutorMemory)
                    .set("spark.executor.cores",sparkExecutorCores)
                    .set("spark.executor.heartbeatInterval",heartbeatIntervalTime)
                    .set("spark.num.executors",sparkExecutorsNum)
                    .set("spark.network.timeout",networkTimeout)
                    .set("spark.rpc.message.maxSize",sparkRpcMessageMaxSize)
                    .set("spark.storage.blockManagerSlaveTimeoutMs",blockManagerSlaveTimeoutMs)
                    .set("spark.driver.host", "localhost").set("spark.testing.memory", "21474800000");
        }else{
            sparkConf
                    .setAppName(appName)
                    .setMaster(sparkMasterUri)
                    .set("spark.executor.memory",sparkExecutorMemory)
                    .set("spark.executor.cores",sparkExecutorCores);
        }


        return sparkConf;
    }

//    @Bean
//    @ConditionalOnMissingBean(JavaSparkContext.class)
//    public JavaSparkContext javaSparkContext()  {
//        JavaSparkContext sc =  new JavaSparkContext(sparkConf());
//
////        if(sparkMasterUri.equals("dev")){
////            sc.addJar(jars);
////        }
//        return sc;
//    }

//    @Bean
//    public SparkSession sparkSession(){
//        return SparkSession
//                .builder()
//                .sparkContext(javaSparkContext().sc())
//                .appName(appName)
//                .getOrCreate();
//    }

//    @Bean
//    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer(){
//        return new PropertySourcesPlaceholderConfigurer();
//    }
}
