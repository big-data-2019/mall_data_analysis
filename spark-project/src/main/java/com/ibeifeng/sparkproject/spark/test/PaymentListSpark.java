package com.ibeifeng.sparkproject.spark.test;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PaymentListSpark implements Serializable {
    public static String dir = "src/main/resources";
    public static FastDateFormat fdf = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss");

    //总记录数
    @Test
    public void count() {
        JavaRDD<String> textFileData = SparkUtil.gettextFileData(dir + "/payment_list.csv");
        long count = textFileData.count();
        System.out.println("count = " + count);
    }

    //钱
    @Test
    public void money() {
        JavaRDD<String> textFileData = SparkUtil.gettextFileData(dir + "/payment_list.csv");
        JavaRDD<Double> map = textFileData.map(new Function<String, Double>() {
            @Override
            public Double call(String v1) throws Exception {
                String[] strings = v1.split(",");
                return Double.parseDouble(strings[5].replaceAll("\"", ""));
            }
        });
        Double allMoney = map.reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("allMoney = " + allMoney);
        //已支付的
        JavaRDD<String> filter = textFileData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split(",");
                boolean nul = strings[9].equals("\"\001\"");
                return nul;
            }
        });
        JavaRDD<Double> map1 = filter.map(new Function<String, Double>() {
            @Override
            public Double call(String v1) throws Exception {
                String[] strings = v1.split(",");
                return Double.parseDouble(strings[5].replaceAll("\"", ""));
            }
        });
        Double isPayMoney = map1.reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("isPayMoney = " + isPayMoney);
        String percentage = SparkUtil.percentage(isPayMoney, allMoney);
        System.out.println("已支付/总金额 = " + percentage);
    }

    //支付类型途径占比
    @Test
    public void payType() {
        JavaRDD<String> textFileData = SparkUtil.gettextFileData(dir + "/payment_list.csv");
        final JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = textFileData.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] strings = s.split(",");
                return new Tuple2<>(strings[10], 1);
            }
        });
        JavaPairRDD<String, Integer> reduceByKey = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, String> sortByKey = integerStringJavaPairRDD.sortByKey(false);
        SparkUtil.printRddToArray(sortByKey);
        //各占比例
        List<Tuple2<Integer, String>> tuple2s = sortByKey.toArray();
        Long allCount = 0L;
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < tuple2s.size(); i++) {
            Tuple2<Integer, String> integerStringTuple2 = tuple2s.get(i);
            allCount += integerStringTuple2._1;
            map.put(integerStringTuple2._2, integerStringTuple2._1);
        }
        //遍历map打印比例
        for(Map.Entry<String,Integer> entry:map.entrySet()){
            String percentage = SparkUtil.percentage(entry.getValue(), allCount);
            System.out.println(entry.getKey()+"所占比例: "+percentage);
        }
    }
    //支付类型占比
    @Test
    public void orderType() {
        JavaRDD<String> textFileData = SparkUtil.gettextFileData(dir + "/payment_list.csv");
        final JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = textFileData.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] strings = s.split(",");
                return new Tuple2<>(strings[6], 1);
            }
        });
        JavaPairRDD<String, Integer> reduceByKey = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, String> sortByKey = integerStringJavaPairRDD.sortByKey(false);
        SparkUtil.printRddToArray(sortByKey);
        //各占比例
        List<Tuple2<Integer, String>> tuple2s = sortByKey.toArray();
        Long allCount = 0L;
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < tuple2s.size(); i++) {
            Tuple2<Integer, String> integerStringTuple2 = tuple2s.get(i);
            allCount += integerStringTuple2._1;
            map.put(integerStringTuple2._2, integerStringTuple2._1);
        }
        //遍历map打印比例
        for(Map.Entry<String,Integer> entry:map.entrySet()){
            String percentage = SparkUtil.percentage(entry.getValue(), allCount);
            System.out.println(entry.getKey()+"所占比例: "+percentage);
        }
    }

}
