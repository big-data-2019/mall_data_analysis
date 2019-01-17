package com.ibeifeng.sparkproject.spark.test.rdd;

import com.ibeifeng.sparkproject.spark.test.utils.SparkUtil;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

//"902","2016/8/31 14:42:05","1","","2016/8/31 15:06:58","2016-08-31-eb407b88-91da-4c2d-8322-347a319dccb0","5","1","成都昭觉寺","114.00"," ","TSPAY201608310002","18782161806",""," ","TS201608310002","2016/8/31 15:30:00","广元南河","topay","","其它","85","0.00","0.00","","","","","","","","","","119.00","ordinary","","2.00","","","114.00","3.00","1","","","","",""
public class TicketSellingSpark implements Serializable {
    public static String dir = "src/main/resources";
    public static FastDateFormat fdf = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss");
    public static JavaRDD<String> textfile = SparkUtil.gettextFileData(dir + "/ticket_selling.csv");
    ;

    //买票总张数
    @Test
    public void count() {
        long count1 = textfile.count();
        JavaRDD<String> javaRDD = textfile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split(",");
                return strings[18].equalsIgnoreCase("ispay");
            }
        });
        long count = javaRDD.count();
        System.out.println("付款的票张数 = " + count);
        System.out.println("所有汽车票订单 = " + count1);
        System.out.println("比例: " + SparkUtil.percentage(count, count1));
    }

    //付款未付款待付款所占比例
    @Test
    public void reduceByKey() {
        long count1 = textfile.count();
        JavaRDD<String> javaRDD = textfile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split(",");
                return strings[18].equalsIgnoreCase("ispay");
            }
        });
        long count = javaRDD.count();
        JavaRDD<String> javaRDD1 = textfile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split(",");
                return strings[18].equalsIgnoreCase("topay");
            }
        });
        long count2 = javaRDD1.count();
        System.out.println("付款的票张数 = " + count);
        System.out.println("所有汽车票订单 = " + count1);
        System.out.println("待支付订单 = " + count2);
        System.out.println("付款比例: " + SparkUtil.percentage(count, count1));
        System.out.println("待支付比例: " + SparkUtil.percentage(count2, count1));
        System.out.println("未支付比例: " + SparkUtil.percentage(count1 - count - count2, count1));
    }

    //到哪儿的人最多
    @Test
    public void startSiteName() {
        JavaPairRDD<Tuple2<String, String>, Integer> mapToPair = textfile.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(String s) throws Exception {
                String[] strings = s.split(",");
                Tuple2<String, String> startEndTuple2 = new Tuple2<String, String>(strings[17], strings[8]);
                return new Tuple2<Tuple2<String, String>, Integer>(startEndTuple2, 1);
            }
        });
        JavaPairRDD<Tuple2<String, String>, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //排序
        JavaPairRDD<Integer, Tuple2<String, String>> integerTuple2JavaPairRDD = reduceByKey.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, Integer, Tuple2<String, String>>() {
            @Override
            public Tuple2<Integer, Tuple2<String, String>> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                return new Tuple2<Integer, Tuple2<String, String>>(tuple2IntegerTuple2._2, tuple2IntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, Tuple2<String, String>> sortByKey = integerTuple2JavaPairRDD.sortByKey(false);
        List<Tuple2<Integer, Tuple2<String, String>>> take = sortByKey.take(1);
        System.out.println("购买最多的路线: ");
        SparkUtil.printList(take);
        SparkUtil.printRddToArray(sortByKey);
    }

    //车型计数
    @Test
    public void carModel() {
        JavaPairRDD<String, Integer> mapToPair = textfile.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] strings = s.split(",");
                return new Tuple2<>(strings[6], 1);
            }
        });
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> mapToPair1 = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, String> sortByKey = mapToPair1.sortByKey(false);
        List<Tuple2<Integer, String>> take = sortByKey.take(1);
        System.out.println("使用最多的车型: ");
        SparkUtil.printList(take);
        SparkUtil.printRddToArray(sortByKey);
    }

    //指定时间卖了多少张
    @Test
    public void countByDate() {
        final String end = "2018/04/18 00:00:00";
        final String start = "2019/01/16 23:22:47";
        long count = countByDate(start, end);
        System.out.println(end + " - " + start + " 一共卖了: " + count);
    }

    //总金额
    @Test
    public void money() {
        JavaRDD<Double> javaRDD = textfile.map(new Function<String, Double>() {
            @Override
            public Double call(String v1) throws Exception {
                Double parseDouble = 0D;
                String[] strings = v1.split(",");
                try {
                    String s = strings[9].replaceAll("\"", "").replaceAll(" ", "");
                    parseDouble = Double.parseDouble(s);
                } catch (Exception e) {
                    System.out.println("错误的参数: " + strings[9]);
                }
                return parseDouble;
            }
        });
        Double money = javaRDD.reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("money = " + money);
    }

    //平均票价
    @Test
    public void avgMoney() {
        JavaRDD<Double> javaRDD = textfile.map(new Function<String, Double>() {
            @Override
            public Double call(String v1) throws Exception {
                Double parseDouble = 0D;
                String[] strings = v1.split(",");
                try {
                    String s = strings[9].replaceAll("\"", "").replaceAll(" ", "");
                    parseDouble = Double.parseDouble(s);
                } catch (Exception e) {
                    System.out.println("错误的参数: " + strings[9]);
                }
                return parseDouble;
            }
        });
        Double money = javaRDD.reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("money = " + money);
        long count = textfile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split(",");
                return !strings[9].equals("0.00");
            }
        }).count();
        System.out.println("count = " + count);
        System.out.println("平均票价: " + (money / count));
    }


    public Long countByDate(final String end, final String start) {
        JavaRDD<String> filter = textfile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split(",");
                String time = strings[1].replaceAll("\"", "");
                Date startDate = fdf.parse(start);
                Date endDate = fdf.parse(end);
                Date timeDate = fdf.parse(time);
                return timeDate.after(startDate) && timeDate.before(endDate);
            }
        });
        return filter.count();
    }

    public static void main(String[] args) throws Exception {
        Date parse = fdf.parse("2018/06/29 09:42:20");
        System.out.println("parse = " + parse);
    }
}
