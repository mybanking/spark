package com.kdy.spark.sparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

// col("...") is preferable to df.col("...")

import  static  org.apache.spark.sql.functions.*;


/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/5/25 10:55
 */
public class SparkSQL {

    public static void main(String[] args) throws AnalysisException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaDecisionTreeClassificationExample")
                .setMaster("local")
                .set("spark.driver.host", "localhost").set("spark.testing.memory", "21474800000");
        // Create a SparkSession.
        JavaSparkContext jsc=new JavaSparkContext(sparkConf);


        SparkSession spark = SparkSession
                .builder()
                .appName("sparkSession")
                .sparkContext(jsc.sc())
                .getOrCreate();

        Dataset<Row> df = spark.read().json("src/main/java/com/kdy/spark/ml/data/people.json");


        df.show();

//        +----+-------+
//| age|   name|
//+----+-------+
//|null|Michael|
//|  30|   Andy|
//|  19| Justin|
//+----+-------+

        // Print the schema in a tree format
        df.printSchema();
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
        df.select("name").show();
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
        df.filter(col("age").gt(21)).show();
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
        df.groupBy("age").count().show();
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+


// $example on:run_sql$
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:run_sql$

        // $example on:global_temp_view$
        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:global_temp_view$



        //Aggregate functions
//计算平均年龄
        df.agg(avg("age")).show();

//        +--------+
//|avg(age)|
//+--------+
//|    24.5|
//+--------+

//Collection functions
        Dataset<Row> arrdf = df.withColumn("info", array("age", "name"));

        arrdf.show();
//        +----+-------+------------+
//| age|   name|        info|
//+----+-------+------------+
//|null|Michael| [, Michael]|
//|  30|   Andy|  [30, Andy]|
//|  19| Justin|[19, Justin]|
//+----+-------+------------+

        arrdf.withColumn("explode",explode(col("info"))).show();

//        +----+-------+------------+-------+
//| age|   name|        info|explode|
//+----+-------+------------+-------+
//|null|Michael| [, Michael]|   null|
//|null|Michael| [, Michael]|Michael|
//|  30|   Andy|  [30, Andy]|     30|
//|  30|   Andy|  [30, Andy]|   Andy|
//|  19| Justin|[19, Justin]|     19|
//|  19| Justin|[19, Justin]| Justin|
//+----+-------+------------+-------+
//计算数组长度
        arrdf.withColumn("size",size(col("info"))).show();

//        +----+-------+------------+----+
//| age|   name|        info|size|
//+----+-------+------------+----+
//|null|Michael| [, Michael]|   2|
//|  30|   Andy|  [30, Andy]|   2|
//|  19| Justin|[19, Justin]|   2|
//+----+-------+------------+----+
//排序
        df.orderBy("age").show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  19| Justin|
        //|  30|   Andy|
        //+----+-------+
        //
// Sorting functions
//降序
        df.orderBy(desc("age")).show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|  30|   Andy|
        //|  19| Justin|
        //|null|Michael|
        //+----+-------+
// Date time functions
//添加日期
        arrdf.withColumn("date", current_date()).show();
//        +----+-------+------------+----------+
//| age|   name|        info|      date|
//+----+-------+------------+----------+
//|null|Michael| [, Michael]|2021-05-25|
//|  30|   Andy|  [30, Andy]|2021-05-25|
//|  19| Justin|[19, Justin]|2021-05-25|
//+----+-------+------------+----------+


    }
}
