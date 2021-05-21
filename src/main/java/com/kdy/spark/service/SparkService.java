package com.kdy.spark.service;

import com.kdy.spark.examples.JavaTC;
import com.kdy.spark.examples.ProjectFn;
import com.kdy.spark.threads.CustomStreamingReceiver;
import com.kdy.spark.util.MyComparator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.sparkproject.guava.base.Joiner;
import org.sparkproject.guava.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;


import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;



/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/4/7 0:51
 */
@Slf4j
@Service
public class SparkService implements Serializable {


    @Value("${spark.jars}")
    private String jars;

    @Autowired
    private   SparkConf sparkConf;

    public Map<String, Object> calculateTopTen(String path,int k) {

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.addJar(jars);


        Map<String, Object> result = new HashMap<>();

        //JavaRDD<String> data = javaSparkContext.textFile("hdfs://mini1:9000/3.txt");//读取hdfs文件
        //JavaRDD<String> data = sc.textFile("C:\3.txt");//读取本地文件
        //JavaRDD<Object> data = sc.parallelize(new ArrayList<>());//读取内存,将list转化为rdd  这个rdd为空,需要提前存入数据在转化



        //text.File  按行读取本地文件
        JavaRDD<String> lines = javaSparkContext.textFile(path).cache();
       // javaSparkContext.setCheckpointDir();

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(lines.count());


        //按空格切割每一条数据返回一个新的rdd new FlatMapFunction<加载后的格式,处理后的格式>()
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) {
                List<String> strings = Arrays.asList(line.split("\\s+"));
                return strings.iterator();
            }
        });



        //将rdd转化成二元的rdd,拼接参数new PairFunction<加载后的key,处理后的key,拼接的value>()
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {//拼接的value
                return new Tuple2<>(word, 1);
            }
        });

        //聚合相同key的rdd并统计次数
        JavaPairRDD<String,Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //分布式缓存数据
        counts.cache();
      //  counts.persist(StorageLevel.MEMORY_AND_DISK());
        //counts.checkpoint();

        //map中key和value的值互换
        JavaPairRDD<Integer, String> temp = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2(),stringIntegerTuple2._1());
            }
        });


      JavaPairRDD<String, Integer> sorted = temp.sortByKey(false).mapToPair(tuple -> new Tuple2<String, Integer>(tuple._2, tuple._1));



      System.out.println();
      System.out.println("-------------------------------------------------------");
      System.out.println(sorted.count());






//       List<Tuple2<String, Integer>> output = sorted.collect();



//        List<Tuple2<String, Integer>> output = sorted.take(10);

        List<Tuple2<String, Integer>> output = sorted.top(k,new MyComparator());

        for (Tuple2<String, Integer> tuple : output) {
            result.put(tuple._1(), tuple._2());
        }

        javaSparkContext.stop();
        return result;
    }

    public  Map<String, Object> javaStreamCalculateTopTen(String path,int k) {

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);

        javaSparkContext.addJar(jars);

        Map<String, Object> result = new HashMap<>();

        //JavaRDD<String> data = javaSparkContext.textFile("hdfs://mini1:9000/3.txt");//读取hdfs文件
        //JavaRDD<String> data = sc.textFile("C:\3.txt");//读取本地文件
        //JavaRDD<Object> data = sc.parallelize(new ArrayList<>());//读取内存,将list转化为rdd  这个rdd为空,需要提前存入数据在转化

        //text.File  按行读取本地文件
        JavaRDD<String> lines = javaSparkContext.textFile(path).cache();

        System.out.println();
        System.out.println("-------------------------------------------------------");

        //行数
        System.out.println(lines.count());

        result.put("总行数",lines.count());




        JavaRDD<String> words = lines.flatMap(str -> Arrays.asList(str.split("\\s+")).iterator());

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(words);


        JavaPairRDD<String, Integer> ones = words.mapToPair(str -> new Tuple2<String, Integer>(str, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> (i1 + i2));


        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(counts.countByValue());
        System.out.println(counts.countByKey());


         JavaPairRDD<Integer, String> temp = counts.mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1));

        JavaPairRDD<String, Integer> sorted = temp.sortByKey(false).mapToPair(tuple -> new Tuple2<String, Integer>(tuple._2, tuple._1));

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(sorted.count());


//       List<Tuple2<String, Integer>> output = sorted.collect();

//        List<Tuple2<String, Integer>> output = sorted.take(10);

        List<Tuple2<String, Integer>> output = sorted.top(k,new MyComparator());

        for (Tuple2<String, Integer> tuple : output) {
            result.put(tuple._1(), tuple._2());
        }
        javaSparkContext.stop();
        return result;
    }


    /**
     * 练习demo，熟悉其中API
     */
    public Map<String, Object> sparkExerciseDemo() {

       JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.addJar(jars);



        Map<String, Object> result = new HashMap<>();

        List<Integer> data = Lists.newArrayList(1, 2, 3, 4, 5, 6);

        JavaRDD<Integer> rdd01 = javaSparkContext.parallelize(data);

        rdd01 = rdd01.map(num -> {
            return num * num;
        });

        //data map :1,4,9,16,25,36
        log.info("data map :{}", Joiner.on(",").skipNulls().join(rdd01.collect()).toString());
        result.put("data map",Joiner.on(",").skipNulls().join(rdd01.collect()).toString());

//        rdd01 = rdd01.filter(x -> x < 6);
        rdd01 = rdd01.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer<6;
            }
        });

        //data filter :1,4
        log.info("data filter :{}", Joiner.on(",").skipNulls().join(rdd01.collect()).toString());
        result.put("data filter",Joiner.on(",").skipNulls().join(rdd01.collect()).toString());


        rdd01 = rdd01.flatMap(x -> {
            Integer[] test = {x, x + 1, x + 2};
            return Arrays.asList(test).iterator();
        });

        //flatMap :1,2,3,4,5,6
        log.info("flatMap :{}", Joiner.on(",").skipNulls().join(rdd01.collect()).toString());

        result.put("flatMap ",Joiner.on(",").skipNulls().join(rdd01.collect()).toString());


        JavaRDD<Integer> unionRdd = javaSparkContext.parallelize(data);

        rdd01 = rdd01.union(unionRdd);

        //union :1,2,3,4,5,6,1,2,3,4,5,6
        log.info("union :{}", Joiner.on(",").skipNulls().join(rdd01.collect()).toString());
        result.put("union ",Joiner.on(",").skipNulls().join(rdd01.collect()).toString());

        List<Integer> result2 = Lists.newArrayList();
        result2.add(rdd01.reduce((Integer v1, Integer v2) -> {
            return v1 + v2;
        }));

        //reduce :42
        log.info("reduce :{}", Joiner.on(",").skipNulls().join(result2).toString());
        result.put("reduce",Joiner.on(",").skipNulls().join(result2).toString());

        result2.forEach(System.out::println);
        result.put("forEach",Joiner.on(",").skipNulls().join(result2).toString());

        JavaPairRDD<String, Iterable<Integer>> groupRdd = rdd01.groupBy(x -> {
            log.info("======grouby========：{}", x);
            if (x > 3) return "大于三";
            else return "小于等于三";
        });

        List<Tuple2<String, Iterable<Integer>>> resultGroup = groupRdd.collect();

        //group by  key:1 value:1,2,3,4,5,6,1,2,3,4,5,6
        resultGroup.forEach(x -> {
            log.info("group by  key:{} value:{}", x._1, Joiner.on(",").skipNulls().join(x._2).toString());
            result.put("group by"+x._1,x._2);
        });

        javaSparkContext.stop();
        return  result;

    }

    /**
     * spark streaming 练习
     */
    public Map<String, Object> sparkStreaming() throws InterruptedException {

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.addJar(jars);

        Map<String, Object> result = new HashMap<>();

        JavaStreamingContext jsc = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));//批间隔时间

        JavaReceiverInputDStream<String> lines = jsc.receiverStream(new CustomStreamingReceiver(StorageLevel.MEMORY_AND_DISK_2()));

        JavaDStream<Long> count = lines.count();
        count = count.map(x -> {
            log.info("这次批一共多少条数据：{}", x);
            result.put("这次批一共多少条数据:"+x,x);
            return x;
        });
        count.print();
        result.put("count",count.toString());
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
        return  result;
    }


    //计算pi
    public Map<String ,Object> calPi(int slices){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);

        javaSparkContext.addJar(jars);

        Map<String, Object> result = new HashMap<>();

        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = javaSparkContext.parallelize(l, slices);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);
        result.put("Pi is roughly:" ,4.0 * count / n);

        javaSparkContext.stop();
        return result;
    }


    //计算传递闭包

    public Map<String ,Object> calTC(int slices){

        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.addJar(jars);
        Map<String, Object> result = new HashMap<>();

        JavaTC javaTC=new JavaTC();

        JavaPairRDD<Integer, Integer> tc = javaSparkContext.parallelizePairs(javaTC.generateGraph(), slices).cache();

        // Linear transitive closure: each round grows paths by one edge,
        // by joining the graph's edges with the already-discovered paths.
        // e.g. join the path (y, z) from the TC with the edge (x, y) from
        // the graph to obtain the path (x, z).

        // Because join() joins on keys, the edges are stored in reversed order.
        JavaPairRDD<Integer, Integer> edges = tc.mapToPair(e -> new Tuple2<>(e._2(), e._1()));

        long oldCount;
        long nextCount = tc.count();
        do {
            oldCount = nextCount;
            // Perform the join, obtaining an RDD of (y, (z, )) pairs,
            // then project the result to obtain the new (x, z) paths.
            tc = tc.union(tc.join(edges).mapToPair(ProjectFn.INSTANCE)).distinct().cache();
            nextCount = tc.count();
        } while (nextCount != oldCount);

        System.out.println("TC has " + tc.count() + " edges.");
        result.put("TC has ",tc.count()+" edgs");
        javaSparkContext.stop();
        return result;
    }
}
