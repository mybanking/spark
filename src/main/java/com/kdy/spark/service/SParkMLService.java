package com.kdy.spark.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/5/17 14:49
 */
@Slf4j
@Service
public class SParkMLService  implements Serializable {

    @Value("${spark.jars}")
    private String jars;

    @Autowired
    private   SparkConf sparkConf;


    public Map<String, Object> kMeans(int k){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        if(jars!=null){
            javaSparkContext.addJar(jars);
        }


        Map<String, Object> result = new HashMap<>();
        // $example on$
        // Load and parse data
        String path = "src/main/java/com/kdy/spark/ml/data/mllib/kmeans_data.txt";


        JavaRDD<String> data = javaSparkContext.textFile(path);

        JavaRDD<org.apache.spark.mllib.linalg.Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return org.apache.spark.mllib.linalg.Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        javaSparkContext.stop();



        return result;
    }


    public Map<String, Object> descisionTree(int maxCategories){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        if(jars!=null){
            javaSparkContext.addJar(jars);
        }

        Map<String, Object> result = new HashMap<>();

        // Load and parse the data file.
        String datapath = "src/main/java/com/kdy/spark/ml/data/mllib/sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(javaSparkContext.sc(), datapath).toJavaRDD();
        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Set parameters.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        int numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        int maxDepth = 5;
        int maxBins = 32;

        // Train a DecisionTree model for classification.
        DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testErr =
                predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();

        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification tree model:\n" + model.toDebugString());

        // $example off$

        javaSparkContext.stop();
        return result;
    }


    public Map<String, Object> linearRegression(){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        if(jars!=null){
            javaSparkContext.addJar(jars);
        }
        SparkSession sparkSession = SparkSession
                .builder().sparkContext(javaSparkContext.sc())
                .appName("JavaKMeansExample")
                .getOrCreate();

        Map<String, Object> result = new HashMap<>();

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
        result.put("Accuracy",accuracy);
        // $example off$

        javaSparkContext.stop();

        return result;
    }


}
