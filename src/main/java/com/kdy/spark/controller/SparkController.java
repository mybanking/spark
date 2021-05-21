package com.kdy.spark.controller;

import com.kdy.spark.service.SparkService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/4/7 1:35
 */
@Api("SparkController")
@RestController
public class SparkController {

    @Autowired
    private SparkService sparkService;

    @ApiOperation("计算topk")
    @GetMapping("/spark/topk")
    public Map<String, Object> calculateTopTen(String path,int k) {
        return sparkService.calculateTopTen(path,k);
    }


    @ApiOperation("计算topk  java 流式方法")
    @GetMapping("/spark/javaStream/topk")
    public Map<String, Object> javaStreamCalculateTopTen(String path,int k) {
        return sparkService.javaStreamCalculateTopTen(path,k);
    }


    @ApiOperation("常见方法demo")
    @GetMapping("/spark/demo")
    public Map<String, Object> demo() {
        return sparkService.sparkExerciseDemo();
    }


    @ApiOperation("spark Streaming")
    @GetMapping("/spark/stream")
    public Map<String, Object> streamingDemo() throws InterruptedException {
        return sparkService.sparkStreaming();
    }

    @ApiOperation("spark PI")
    @GetMapping("/spark/pi")
    public Map<String, Object> sparkPi(int slice) throws InterruptedException {
        return sparkService.calPi(slice);
    }



    @ApiOperation(" 传递闭包")
    @GetMapping("/spark/TC")
    public Map<String, Object> sparkTC(int slice) throws InterruptedException {
        return sparkService.calTC(slice);
    }
}