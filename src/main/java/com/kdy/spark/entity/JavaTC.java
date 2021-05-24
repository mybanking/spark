package com.kdy.spark.entity;

import lombok.Data;
import scala.Tuple2;

import java.util.*;

/**
 * @PROJECT_NAME: spark
 * @DESCRIPTION:
 * @USER: 14020
 * @DATE: 2021/4/13 13:33
 */
@Data
public  class JavaTC {

    private   int numEdges = 200;
    private   int numVertices = 100;
    private   Random rand = new Random(42);

     public List<Tuple2<Integer, Integer>> generateGraph() {
        Set<Tuple2<Integer, Integer>> edges = new HashSet<>(numEdges);
        while (edges.size() < numEdges) {
            int from = rand.nextInt(numVertices);
            int to = rand.nextInt(numVertices);
            Tuple2<Integer, Integer> e = new Tuple2<>(from, to);
            if (from != to) {
                edges.add(e);
            }
        }
        return new ArrayList<>(edges);
    }

}

