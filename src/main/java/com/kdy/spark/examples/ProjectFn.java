package com.kdy.spark.examples;

import lombok.Data;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/4/13 13:41
 */
@Data
public class ProjectFn implements PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>,
        Integer, Integer> {
    public static ProjectFn INSTANCE = new ProjectFn();

    @Override
    public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> triple) {
        return new Tuple2<>(triple._2()._2(), triple._2()._1());
    }
}
