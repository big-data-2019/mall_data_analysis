package com.ibeifeng.sparkproject.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RDDJavaTest implements Serializable {
    /**
     *
     * @desc MAP 算子
     * @author 大水怪
     * @date 2019/1/8 下午 09:47
     * @param []
     * @return void
     */
    @Test
    public  void map() {
        //创建SparkConf  
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local");

        //创建JavasparkContext  
        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造集合  
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        //并行化集合，创建初始RDD  
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //使用map算子，将集合中的每个元素都乘以2  
        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        //打印新的RDD  
        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });
        //关闭JavasparkContext  
        sc.close();
    }
    /**
     *
     * @desc filter
     * @author 大水怪
     * @date 2019/1/8 下午 09:47
     * @param []
     * @return void
     */
    @Test
    public  void filter() {
        //创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");

        //创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //模拟集合
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //对集合使用filter算子，过滤出集合中的偶数
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1%2==0;
            }
        });
        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }

        });
        sc.close();
    }
    /**
     *
     * @desc flatMap
     * @author 大水怪
     * @date 2019/1/8 下午 09:48
     * @param []
     * @return void
     */
    @Test
    public  void flatMap() {
        SparkConf conf = new SparkConf()
                .setAppName("flatMap")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lineList = Arrays.asList("hello you","hello me","hello world");

        JavaRDD<String> lines = sc.parallelize(lineList);

        //对RDD执行flatMap算子，将每一行文本，拆分为多个单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            //在这里，传入第一行，hello,you
            //返回的是一个Iterable<String>(hello,you)
            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });
        sc.close();
    }
    /**
     *  
     * @desc  groupByKey
     * @author 大水怪
     * @date 2019/1/8 下午 09:48
     * @param []
     * @return void
     */
    @Test
    public  void groupByKey() {
        SparkConf conf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 90),
                new Tuple2<String, Integer>("class1", 97),
                new Tuple2<String, Integer>("class2", 89));

        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        //针对scoresRDD，执行groupByKey算子，对每个班级的成绩进行分组
        //相当于是，一个key join上的所有value，都放到一个Iterable里面去了
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();
        groupedScores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t)
                    throws Exception {
                System.out.println("class:" + t._1);
                Iterator<Integer> ite = t._2.iterator();
                while(ite.hasNext()) {
                    System.out.println(ite.next());
                }
            }
        });
    }
    /**
     *  
     * @desc sortByKey
     * @author 大水怪
     * @date 2019/1/8 下午 09:49
     * @param []
     * @return void
     */
    @Test
    public  void sortByKey() {
        SparkConf conf = new SparkConf()
                .setAppName("sortByKey")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(78, "marry"),
                new Tuple2<Integer, String>(89, "tom"),
                new Tuple2<Integer, String>(72, "jack"),
                new Tuple2<Integer, String>(86, "leo"));

        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);

        JavaPairRDD<Integer, String> sortedScores = scores.sortByKey();
        sortedScores.foreach(new VoidFunction<Tuple2<Integer,String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + ":" + t._2);
            }
        });
        sc.close();
    }
    /**
     *  
     * @desc join
     * @author 大水怪
     * @date 2019/1/8 下午 09:58
     * @param []
     * @return void
     */
    @Test
    public  void join() {
        SparkConf conf = new SparkConf()
                .setAppName("join")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "tom"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "marry"),
                new Tuple2<Integer, String>(4, "leo"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 78),
                new Tuple2<Integer, Integer>(2, 87),
                new Tuple2<Integer, Integer>(3, 89),
                new Tuple2<Integer, Integer>(4, 98));

        //并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);;
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        //使用join算子关联两个RDD
        //join以后，会根据key进行join，并返回JavaPairRDD
        //JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key类型，因为通过key进行join的
        //第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);

        //打印
        studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
                    throws Exception {
                System.out.println("student id:" + t._1);
                System.out.println("student name:" + t._2._1);
                System.out.println("student score:" + t._2._2);
                System.out.println("==========================");
            }
        });
        sc.close();
    }
}
