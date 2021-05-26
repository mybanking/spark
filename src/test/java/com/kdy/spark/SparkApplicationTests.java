package com.kdy.spark;

import com.kdy.spark.service.SParkMLService;
import com.kdy.spark.service.SparkService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

@SpringBootTest
class SparkApplicationTests {


	@Autowired
	private SparkService sparkService;

	@Autowired
	private SParkMLService sParkMLService;

	@Test
	void sparkExerciseDemo() {

		Map<String, Object> map= sparkService.sparkExerciseDemo();
		System.out.println(map.toString());
	}


	@Test
	void topTen(){
		Map<String, Object> map=sparkService.calculateTopTen("src/main/resources/demo/kdy.txt",5);
		System.out.println(map.toString());
	}

	@Test
	void Pi(){
		Map<String, Object> map= sparkService.calPi(10);
		System.out.println(map.toString());
	}


	@Test
	void sparkKMeans(){
		Map<String, Object> map= sParkMLService.kMeans(5);
		System.out.println(map.toString());
	}



	@Test
	void DecisionTree(){
		Map<String, Object> map= sParkMLService.descisionTree(3);
		System.out.println(map.toString());
	}


	@Test
	void LinearRegression(){
		Map<String, Object> map= sParkMLService.linearRegression();
		System.out.println(map.toString());
	}






}
