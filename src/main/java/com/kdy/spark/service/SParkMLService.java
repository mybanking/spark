package com.kdy.spark.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
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
        javaSparkContext.addJar(jars);
        SparkSession sparkSession = SparkSession
                .builder().sparkContext(javaSparkContext.sc())
                .appName("JavaKMeansExample")
                .getOrCreate();

        Map<String, Object> result = new HashMap<>();
        // $example on$
        // Loads data.
        Dataset<Row> dataset = sparkSession.read().format("libsvm").load("src/main/resources/data/mllib/sample_kmeans_data.txt");

        // Trains a bisecting k-means model.
        BisectingKMeans bkm = new BisectingKMeans().setK(k).setSeed(1);
        BisectingKMeansModel model = bkm.fit(dataset);

        // Make predictions
        Dataset<Row> predictions = model.transform(dataset);

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);
        result.put("Silhouette with squared euclidean distance = " ,silhouette);

        // Shows the result.
        System.out.println("Cluster Centers: ");
        String s="";
        Vector[] centers = model.clusterCenters();
        for (Vector center : centers) {
            System.out.println(center);
            s=s+center.toString();

        }
        result.put("Cluster Centers: ",s);

        sparkSession.stop();



        return result;
    }


    public Map<String, Object> descisionTree(int maxCategories){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.addJar(jars);
        SparkSession sparkSession = SparkSession
                .builder().sparkContext(javaSparkContext.sc())
                .appName("JavaKMeansExample")
                .getOrCreate();


        Map<String, Object> result = new HashMap<>();

        // $example on$
        // Load the data stored in LIBSVM format as a DataFrame.
        Dataset<Row> data = sparkSession
                .read()
                .format("libsvm")
                .load("src/main/java/com/kdy/spark/ml/data/mllib/sample_libsvm_data.txt");

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);

        // Automatically identify categorical features, and index them.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
                .fit(data);

        // Split the data into training and test sets (30% held out for testing).
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Train a DecisionTree model.
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labelsArray()[0]);

        // Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

        // Select (prediction, true label) and compute test error.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1.0 - accuracy));
        result.put("Test Error = " ,(1.0 - accuracy));

        DecisionTreeClassificationModel treeModel =
                (DecisionTreeClassificationModel) (model.stages()[2]);
        System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
        result.put("Learned classification tree model: " ,treeModel.toDebugString());
        sparkSession.stop();
        return result;
    }


    public Map<String, Object> linearRegression(){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.addJar(jars);
        SparkSession sparkSession = SparkSession
                .builder().sparkContext(javaSparkContext.sc())
                .appName("JavaKMeansExample")
                .getOrCreate();

        Map<String, Object> result = new HashMap<>();

        // $example on$
        // Load training data.
        Dataset<Row> training = sparkSession.read().format("libsvm")
                .load("src/main/java/com/kdy/spark/ml/data/mllib/sample_linear_regression_data.txt");

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // Fit the model.
        LinearRegressionModel lrModel = lr.fit(training);

        // Print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
        result.put("Coefficients: ",lrModel.coefficients().toString());
        result.put("Intercept: " ,lrModel.intercept());
        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        result.put("numIterations: " ,trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        result.put("objectiveHistory: " ,Vectors.dense(trainingSummary.objectiveHistory()).toString());
        trainingSummary.residuals().show();
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        result.put("RMSE: " , trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());
        result.put("r2: " , trainingSummary.r2());
        // $example off$
        sparkSession.stop();

        return result;
    }


}
