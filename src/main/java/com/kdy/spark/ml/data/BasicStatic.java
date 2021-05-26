package com.kdy.spark.ml.data;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.SparkConf;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/5/26 13:50
 */
public class BasicStatic {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("BasicStatic");
        sparkConf.setMaster("local")
                .set("spark.driver.host", "localhost").set("spark.testing.memory", "21474800000");

        JavaSparkContext jsc=new JavaSparkContext(sparkConf);

        JavaRDD<Vector> mat = jsc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(3.0, 30.0, 300.0)
                )
        ); // an RDD of Vectors

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        System.out.println("输出"+summary.mean());  // 包含每列平均值的密集向量
        System.out.println("输出"+summary.variance());  // 列方差
        System.out.println("输出"+summary.numNonzeros());  // 每列中的非零数量

    }


}
