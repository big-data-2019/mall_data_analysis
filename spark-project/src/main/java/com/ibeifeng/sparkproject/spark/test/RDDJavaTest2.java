package com.ibeifeng.sparkproject.spark.test;

import groovy.lang.Tuple;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class RDDJavaTest2 implements Serializable {
    public static String dir = "src/main/resources";

    @Test
    public void map() {
        JavaSparkContext sc = getJavaSparkContext();
        List<Integer> integers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            integers.add(i);
        }
        JavaRDD<Integer> parallelize = sc.parallelize(integers);
        JavaRDD<Integer> doubleParallelize = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        doubleParallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    @Test
    public void textFile() {
        //1,jack,20
        JavaSparkContext javaSparkContext = getJavaSparkContext();
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("src/main/resources/test.txt");
        JavaRDD<Object> objectJavaRDD = stringJavaRDD.map(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                return s.split(",");
            }
        });
        printRdd(objectJavaRDD);
    }

    @Test
    public void flatMap() {
        JavaSparkContext javaSparkContext = getJavaSparkContext();
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("src/main/resources/test.txt");
        JavaRDD<Object> objectJavaRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterable<Object> call(String s) throws Exception {
                List strings = Arrays.asList(s.split(","));
                return strings;
            }
        });
        printRdd(objectJavaRDD);
    }

    @Test
    public void filter() {
        JavaSparkContext javaSparkContext = getJavaSparkContext();
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("src/main/resources/test.txt");
        JavaRDD<Object> objectJavaRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterable<Object> call(String s) throws Exception {
                List strings = Arrays.asList(s.split(","));
                return strings;
            }
        });
        JavaRDD<Object> jack = objectJavaRDD.filter(new Function<Object, Boolean>() {
            @Override
            public Boolean call(Object o) throws Exception {
                String s = String.valueOf(o);
                return !s.equals("jack");
            }
        });
        printRdd(jack);
    }

    @Test
    public void wordCount() {
        JavaSparkContext javaSparkContext = getJavaSparkContext();

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(dir + "/test.txt");
        JavaRDD<Object> objectJavaRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterable<Object> call(String s) throws Exception {
                List strings = Arrays.asList(s.split(","));
                return strings;
            }
        });
        JavaRDD<Tuple2> tuple2Rdd = objectJavaRDD.map(new Function<Object, Tuple2>() {
            @Override
            public Tuple2 call(Object o) throws Exception {
                return new Tuple2<Object, Integer>(o, 1);
            }
        });
        printRdd(tuple2Rdd);
        final HashMap<Object, Integer> stringObjectHashMap = new HashMap<>();
        JavaRDD<Object> map = tuple2Rdd.map(new Function<Tuple2, Object>() {
            @Override
            public Object call(Tuple2 tuple2) throws Exception {
                if (stringObjectHashMap.get(tuple2._1) == null) {
                    stringObjectHashMap.put(tuple2._1, 1);
                } else {
                    stringObjectHashMap.put(tuple2._1, stringObjectHashMap.get(tuple2._1) + 1);
                }
                return stringObjectHashMap;
            }
        });
        printRdd(map);
    }

    public JavaSparkContext getJavaSparkContext() {
        //创建SparkConf
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        //创建JavasparkContext
        return new JavaSparkContext(conf);
    }

    public void printRdd(JavaRDD javaRDD) {
        javaRDD.foreach(new VoidFunction() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Object o) throws Exception {
                if (o instanceof Object[]) {
                    Object[] o1 = (Object[]) o;
                    for (int i = 0; i < o1.length; i++) {
                        Object o2 = o1[i];
                        System.out.print(o2 + " ");
                    }
                } else if (o instanceof List) {
                    List o2 = (List) o;
                    for (int i = 0; i < o2.size(); i++) {
                        Object o1 = o2.get(i);
                        System.out.print(o1 + " ");
                    }
                } else if (o instanceof Map) {
                    Map<Object, Object> o1 = (Map<Object, Object>) o;
                    Set<Map.Entry<Object, Object>> entries = o1.entrySet();
                    for (Map.Entry<Object, Object> entry : entries) {
                        System.out.println(entry.getKey() + "=" + entry.getValue());
                    }
                } else {
                    System.out.print(o);
                }
                System.out.println();
            }
        });
    }

    public void printRdd(JavaPairRDD javaRDD) {
        javaRDD.foreach(new VoidFunction() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Object o) throws Exception {
                if (o instanceof Object[]) {
                    Object[] o1 = (Object[]) o;
                    for (int i = 0; i < o1.length; i++) {
                        Object o2 = o1[i];
                        System.out.print(o2 + " ");
                    }
                } else if (o instanceof List) {
                    List o2 = (List) o;
                    for (int i = 0; i < o2.size(); i++) {
                        Object o1 = o2.get(i);
                        System.out.print(o1 + " ");
                    }
                } else if (o instanceof Map) {
                    Map<Object, Object> o1 = (Map<Object, Object>) o;
                    Set<Map.Entry<Object, Object>> entries = o1.entrySet();
                    for (Map.Entry<Object, Object> entry : entries) {
                        System.out.println(entry.getKey() + "=" + entry.getValue());
                    }
                } else {
                    System.out.print(o);
                }
                System.out.println();
            }
        });
    }
}
