package com.kdy.spark.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/5/26 13:21
 */


public class VectorDemo {
    public static void main(String[] args) {

// Create a dense vector (1.0, 0.0, 3.0).
        Vector dv = Vectors.dense(1.0, 0.0, 3.0);

// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
        Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});

        System.out.println(dv.toString());
        System.out.println(sv.toString());



// Create a labeled point with a positive label and a dense feature vector.
        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

// Create a labeled point with a negative label and a sparse feature vector.
        LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));

        System.out.println(pos.toString());
        System.out.println(neg.toString());



        SparkConf sparkConf = new SparkConf().setAppName("VectorDemo");


       sparkConf.setMaster("local")
                    .set("spark.driver.host", "localhost").set("spark.testing.memory", "21474800000");

        JavaSparkContext jsc=new JavaSparkContext(sparkConf);

        JavaRDD<LabeledPoint> examples =
                MLUtils.loadLibSVMFile(jsc.sc(), "src/main/java/com/kdy/spark/ml/data/mllib/sample_libsvm_data.txt").toJavaRDD();

    }
}
