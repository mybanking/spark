package com.kdy.spark.util;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/4/7 14:15
 */
public class MyComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        return o1._2()> o2._2()?1:0;
    }
}
