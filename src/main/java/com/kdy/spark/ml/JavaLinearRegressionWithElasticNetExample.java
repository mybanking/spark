/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kdy.spark.ml;

// $example on$
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
// $example off$

public class JavaLinearRegressionWithElasticNetExample {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf()
            .setAppName("JavaLinearRegressionWithElasticNetExample")
            .setMaster("local")
            .set("spark.driver.host", "localhost").set("spark.testing.memory", "21474800000");
    // Create a SparkSession.
    JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);

    // $example on$
    String path = "src/main/java/com/kdy/spark/ml/data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(javaSparkContext.sc(), path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    JavaRDD<LabeledPoint> test = splits[1];

    // Run training algorithm to build the model.
    LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
            .setNumClasses(10)
            .run(training.rdd());

    // Compute raw scores on the test set.
    JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
            new Tuple2<>(model.predict(p.features()), p.label()));

    // Get evaluation metrics.
    MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
    double accuracy = metrics.accuracy();
    System.out.println("Accuracy = " + accuracy);
    // $example off$

    javaSparkContext.stop();
  }
}