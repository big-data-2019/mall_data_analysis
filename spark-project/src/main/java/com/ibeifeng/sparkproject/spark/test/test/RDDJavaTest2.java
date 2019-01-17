package com.ibeifeng.sparkproject.spark.test.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class RDDJavaTest2 implements Serializable {
    public static String dir = "src/main/resources";
    public static String hdfsDir = "hdfs://localhost:9000/testData";

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
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(dir + "/test.txt");
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
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();
        printRdd(objectJavaRDD);
    }

    @Test
    public void filter() {
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();
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
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();
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

    @Test
    public void countAndTake() {
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();
        long count = objectJavaRDD.count();
        System.out.println("count = " + count);
        List<Object> take = objectJavaRDD.take(2);
        for (int i = 0; i < take.size(); i++) {
            Object o = take.get(i);
            System.out.println("o = " + o);
        }

    }

    @Test
    public void saveAsTextFile() {
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();

        objectJavaRDD.saveAsTextFile(hdfsDir + "/testRdd");

    }

    @Test
    public void countByKey() {
        JavaPairRDD<String, Integer> pairRDD = getStringIntegerJavaPairRDDData();
        Map<String, Object> map = pairRDD.countByKey();
        printMap(map);
    }

    @Test
    public void reduce() {
        JavaRDD<Object> objectJavaRDDData = getObjectJavaRDDData();
        Object reduce = objectJavaRDDData.reduce(new Function2<Object, Object, Object>() {
            @Override
            public Object call(Object o, Object o2) throws Exception {
                return o.toString() + o2;
            }
        });
        System.out.println("reduce = " + reduce);
    }

    @Test
    public void reduceByKey() {
        JavaPairRDD<String, Integer> pairRDD = getStringIntegerJavaPairRDDData();
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        printRdd(reduceByKey);
    }


    //工具
    private JavaPairRDD<String, Integer> getStringIntegerJavaPairRDDData() {
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();
        return objectJavaRDD.mapToPair(new PairFunction<Object, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Object o) throws Exception {
                return new Tuple2<String, Integer>(o.toString(), 1);
            }
        });
    }

    //得到测试 RDD
    public JavaRDD<Object> getObjectJavaRDDData(String path) {
        JavaSparkContext javaSparkContext = getJavaSparkContext();
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(path);
        return stringJavaRDD.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterable<Object> call(String s) throws Exception {
                List strings = Arrays.asList(s.split(","));
                return strings;
            }
        });
    }

    //得到测试 RDD
    public JavaRDD<Object> getObjectJavaRDDData() {
        return getObjectJavaRDDData(dir + "/test.txt");
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

    public void printMap(Map map) {
        Set<Map.Entry<Object, Object>> sets = map.entrySet();
        for (Map.Entry<Object, Object> kv : sets) {
            System.out.println(kv.getKey() + "=" + kv.getValue());
        }
    }
}
