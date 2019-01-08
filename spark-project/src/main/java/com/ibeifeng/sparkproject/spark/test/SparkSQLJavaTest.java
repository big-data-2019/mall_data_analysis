package com.ibeifeng.sparkproject.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SparkSQLJavaTest implements Serializable{
    @Test
    public void sparkSQL(){
        SparkConf conf = new SparkConf().setAppName("HelloWorld").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //写入的数据内容
        JavaRDD<String> personData = sc.parallelize(Arrays.asList("java chinese 5", "c++ chinese 6"));
        //数据库内容
        String url = "jdbc:mysql://localhost:3306/test";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
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
        personsDF.write().mode("append").jdbc(url, "person", connectionProperties);

        sc.close();
    }
    /**
     *
     * @desc 读取数据
     * @author 大水怪
     * @date 2019/1/8 下午 10:40
     * @param []
     * @return void
     */
    @Test
    public void getgetTagByDay(){
        SparkConf conf = new SparkConf().setAppName("HelloWorld").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        getTagByDay(sqlContext);
    }
    public void getTagByDay(SQLContext sqlContext) {
        String url = "jdbc:mysql://127.0.0.1:3306/test";
        //查找的表名
        String table = "person";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的person表内容");

        // 读取表中所有数据
        sqlContext.read().jdbc(url, table, connectionProperties);
        DataFrame jd = sqlContext.sql("SELECT * FROM person  ");
        //显示数据
        jd.show();

    }

}
