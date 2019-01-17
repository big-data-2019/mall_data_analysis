package com.ibeifeng.sparkproject.spark.test.test;

import com.ibeifeng.sparkproject.spark.test.pojo.Language;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataFrameJavaTest implements Serializable {
    @Test
    public void sparkSQL() {
        SparkConf conf = new SparkConf().setAppName("HelloWorld").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //写入的数据内容
        JavaRDD<String> personData = sc.parallelize(Arrays.asList("java chinese 5", "c++ chinese 6"));
       /**
         * 第一步：在RDD的基础上创建类型为Row的RDD
         */
        //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
        JavaRDD<Row> personsRDD = personData.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(splited[0], splited[1], Integer.valueOf(splited[2]));
            }
        });

        /**
         * 第二步：动态构造DataFrame的元数据。
         */
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("search_word", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("hot_index", DataTypes.IntegerType, true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         */
        DataFrame personsDF = sqlContext.createDataFrame(personsRDD, structType);
        /**
         * 第四步：将数据写入到person表中
         */
        personsDF.registerTempTable("language");
        DataFrame sqlDF = sqlContext.sql("select * from language");
        sqlDF.show();
        JavaRDD<Row> rowJavaRDD = sqlDF.javaRDD();
        final List<Language> languages = new ArrayList<>();
        JavaRDD<Language> languageJavaRDD = rowJavaRDD.map(new Function<Row, Language>() {
            @Override
            public Language call(Row v1) throws Exception {
                Language language = new Language(v1.getString(0), v1.getString(1), v1.getInt(2));
                languages.add(language);
                System.out.println(language);
                return language;
            }
        });
        List<Language> collect = languageJavaRDD.collect();
        for (int i = 0; i < collect.size(); i++) {
            Language language =  collect.get(i);
            System.out.println("collect-language = " + language);
        }
        for (int i = 0; i < languages.size(); i++) {
            Language language =  languages.get(i);
            System.out.println("language = " + language);
        }
        sc.close();
    }
}
