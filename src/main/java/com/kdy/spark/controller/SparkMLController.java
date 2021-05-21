package com.kdy.spark.controller;

import com.kdy.spark.service.SParkMLService;
import com.kdy.spark.service.SparkService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/5/17 14:48
 */
@Api("SparkMLController")
@RestController
public class SparkMLController {


    @Autowired
    private SParkMLService sParkMLService;

    @ApiOperation(" kMeans")
    @GetMapping("/spark/ML/Kmeans")
    public  Map<String, Object> sparkKMeans(int k) throws InterruptedException {
        return sParkMLService.kMeans(k);
    }

    @ApiOperation(" DecisionTree")
    @GetMapping("/spark/ML/DecisionTree")
    public  Map<String, Object> DecisionTree(int maxCategories) throws InterruptedException {
        return sParkMLService.descisionTree(maxCategories);
    }

    @ApiOperation(" LinearRegression")
    @GetMapping("/spark/ML/LinearRegression")
    public  Map<String, Object> LinearRegression() throws InterruptedException {
        return sParkMLService.linearRegression();
    }
}
