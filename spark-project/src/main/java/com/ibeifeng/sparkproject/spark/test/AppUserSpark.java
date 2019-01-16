package com.ibeifeng.sparkproject.spark.test;

import com.ibeifeng.sparkproject.spark.test.sort.PayNumSort;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
//"83","2016/8/29 18:28:19","1","","2019/1/7 16:38:50","/headPortrait/83/d833c895d143ad4bd74bac3f85025aafa50f06e3.jpg","","冲哥哥","4PHwE7hpihMXTtWJTlLayPIXghBn7LFEt6yRRJ/Qh13NPLTgSnmxfrMxnq5P5CBZud6WBd8Skd7t4B/TaMgMEw==","1","ee307b42ced04b37b1679149ec9bffcf","2731","84","","07591432","45","formal","21","",""
public class AppUserSpark implements Serializable {
    public static String dir = "src/main/resources";

    //计算注册人数
    @Test
    public void registerCount() {
        JavaRDD<String> textFile = getAppUserRDD();
        long count = textFile.count();
        System.out.println("注册总人数 = " + count);
    }

    //计算非公共账号 占比
    @Test
    public void noPublicAppUser() {
        JavaRDD<String> textFile = getAppUserRDD();
        long count = textFile.count();
        System.out.println("注册总人数 = " + count);
        //得到数组
        JavaRDD<String[]> stringsRdd = textFile.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(",");
            }
        });
        //得到不是公共账号的个数
        JavaRDD<String[]> noPublicRdd = stringsRdd.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                String string = strings[16];
                return string.equals("\"noPublic\"");
            }
        });
        long count1 = noPublicRdd.count();
        System.out.println("noPublic = " + count1);
        //所占比例
        System.out.println(count1 + "/" + count + " === 所占百分比 " + String.format("%.2f", (count1 / Double.parseDouble(count + "")) * 100) + "%");
    }

    //计算各个 消费过的用户
    @Test
    public void payNum() {
        JavaRDD<String> textFile = getAppUserRDD();
        long count = textFile.count();
        System.out.println("注册总人数 = " + count);
        //得到数组
        JavaRDD<String[]> stringsRdd = textFile.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(",");
            }
        });
        JavaPairRDD<String, Integer> mapToPair = stringsRdd.mapToPair(new PairFunction<String[], String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String[] strings) throws Exception {
                return new Tuple2<>(strings[15],1);
            }
        });
        //计算各个支付次数
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //类排序
        JavaPairRDD<String, Integer> sortByKey = reduceByKey.sortByKey(new PayNumSort());
         SparkUtil.printRddToArray(sortByKey);
        //根据KEY 一次排序 交换位置
        JavaPairRDD<Integer, String> mapToPair1 = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, String> sortByKey1 = mapToPair1.sortByKey();
        SparkUtil.printRddToArray(sortByKey1);
    }
    //计算微信使用用户
    @Test
    public void weixinUser(){
        JavaRDD<String> textFile = getAppUserRDD();
        long count = textFile.count();
        System.out.println("注册总人数 = " + count);
        //得到数组
        JavaRDD<String[]> stringsRdd = textFile.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(",");
            }
        });
        //得到不是公共账号的个数
        JavaRDD<String[]> noPublicRdd = stringsRdd.filter(new Function<String[], Boolean>() {
            @Override
            public Boolean call(String[] strings) throws Exception {
                String string = strings[13];
                return !"\"\"".equals(string);
            }
        });
        long count1 = noPublicRdd.count();
        System.out.println("weixin = " + count1);
        //所占比例
        System.out.println(count1 + "/" + count + " === 所占百分比 " + String.format("%.2f", (count1 / Double.parseDouble(count + "")) * 100) + "%");

    }

    private JavaRDD<String> getAppUserRDD() {
        JavaSparkContext javaSparkContext = SparkUtil.getJavaSparkContext();
        return javaSparkContext.textFile(dir + "/app_user.csv");
    }

}
