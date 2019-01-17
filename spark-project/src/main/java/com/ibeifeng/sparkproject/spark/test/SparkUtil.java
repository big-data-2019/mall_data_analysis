package com.ibeifeng.sparkproject.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class SparkUtil {
    public static String dir = "src/main/resources";
    public static String hdfsDir = "hdfs://localhost:9000/testData";

    //工具
    public static JavaPairRDD<String, Integer> getStringIntegerJavaPairRDDData() {
        JavaRDD<Object> objectJavaRDD = getObjectJavaRDDData();
        return objectJavaRDD.mapToPair(new PairFunction<Object, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Object o) throws Exception {
                return new Tuple2<String, Integer>(o.toString(), 1);
            }
        });
    }

    //得到测试 RDD

    public static JavaRDD<Object> getObjectJavaRDDData() {
        return getObjectJavaRDDData(dir + "/test.txt");
    }

    public static JavaRDD<Object> getObjectJavaRDDData(String path) {
        return getObjectJavaRDDData(path, ",");
    }

    public static JavaRDD<Object> getObjectJavaRDDData(String path, final String reg) {
        JavaSparkContext javaSparkContext = getJavaSparkContext();
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(path);
        return stringJavaRDD.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public Iterable<Object> call(String s) throws Exception {
                List strings = Arrays.asList(s.split(reg));
                return strings;
            }
        });
    }

    //得到JavaSparkContext 对象
    public static JavaSparkContext getJavaSparkContext() {
        return getJavaSparkContext("test", "local[*]");
    }

    public static JavaSparkContext getJavaSparkContext(String appName, String master) {
        //创建SparkConf
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        //创建JavasparkContext
        return new JavaSparkContext(conf);
    }

    //得到textfile RDD
    public static JavaRDD<String> gettextFileData(String path) {
        return SparkUtil.getJavaSparkContext().textFile(path);
    }

    public static void printRddToArray(JavaRDD javaRDD) {
        Object[] objects = javaRDD.collect().toArray();
        printArray(objects);
    }


    public static void printRddToArray(JavaPairRDD javaPairRDD) {
        Object[] objects = javaPairRDD.collect().toArray();
        printArray(objects);
    }

    public static void printRdd(JavaRDD javaRDD) {
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
                    printMap(o1);
                } else {
                    System.out.print(o);
                }
                System.out.println();
            }
        });
    }

    public static void printRdd(JavaPairRDD javaRDD) {
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

    public static void printMap(Map map) {
        Set<Map.Entry<Object, Object>> sets = map.entrySet();
        for (Map.Entry<Object, Object> kv : sets) {
            System.out.println(kv.getKey() + "=" + kv.getValue());
        }
    }

    public static void printArray(Object[] objects) {
        System.out.print("[");
        for (int i = 0; i < objects.length; i++) {
            Object object = objects[i];
            System.out.print(object);
            if (objects.length - 1 != i) {
                System.out.print(",");
            }
        }
        System.out.print("]");
        System.out.println();
    }

    public static String percentage(Number child, Number parent) {
        return String.format("%.2f", (child.doubleValue() / parent.doubleValue()) * 100) + "%";
    }

    public static String percentage(String child, String parent) {
        return percentage(new BigDecimal(Double.parseDouble(child)), new BigDecimal(Double.parseDouble(parent)));
    }

    public static void printList(List take) {
        for (int i = 0; i < take.size(); i++) {
            Object o = take.get(i);
            if (o instanceof List) {
                printList((List) o);
            } else {
                System.out.println(o);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String a = "a";
        String b = "b";
        String c = "c";
        String d = "d";
        StringBuffer sb = new StringBuffer();
        sb.append(a).append("\001").append(b).append("\001").append(c).append("\001").append(d);
        FileWriter fileWriter = new FileWriter("C:\\Users\\Administrator\\Desktop\\test.txt");
        fileWriter.write(sb.toString());
        fileWriter.flush();
    }
}
