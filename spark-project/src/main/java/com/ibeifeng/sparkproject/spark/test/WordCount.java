package com.ibeifeng.sparkproject.spark.test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.sources.In;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class WordCount implements Serializable {
    private static final long serialVersionUID = 1L;

    @Test
    public void wordcount() {
        JavaSparkContext javaSparkContext = new RDDJavaTest2().getJavaSparkContext();
        JavaRDD<String> textFileRDD = javaSparkContext.textFile(RDDJavaTest2.dir + "/test.txt");
        JavaRDD<String> flatMapRDD = textFileRDD.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });
        JavaPairRDD<String, Integer> mapToPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceByKeyRDD = mapToPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        new RDDJavaTest2().printRdd(reduceByKeyRDD);
    }
}
