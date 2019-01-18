package com.ibeifeng.sparkproject.spark.test.sql;

import com.ibeifeng.sparkproject.spark.test.pojo.AppUser2;
import com.ibeifeng.sparkproject.spark.test.pojo.TicketSelling;
import com.ibeifeng.sparkproject.spark.test.utils.CommonUtil;
import com.ibeifeng.sparkproject.spark.test.utils.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

public class TicketSellingSQL implements Serializable {
    public static String dir = "src/main/resources";
    //javaspark上下文对象
    public static JavaSparkContext javaSparkContext = SparkUtil.getJavaSparkContext();
    //spark Sql对象
    public static SQLContext sqlContext = new SQLContext(javaSparkContext);
    //javaRdd<String>
    public static JavaRDD<String> textFile = SparkUtil.gettextFileData(javaSparkContext, dir + "/ticket_selling.csv");
    //javaRdd<Row> 相当于数据库表的一行
//    public static JavaRDD<Row> rowRDD = SparkUtil.stringRddToRowRdd(textFile);
    public static JavaRDD<Row> rowRDD = SparkUtil.rddToRowRdd(textFile, TicketSelling.class);

    @Test
    public void count() {
        //初始化 DataFrame
        getAppUserDataFrame("ticket_selling");
        //总人数
        DataFrame countDF = sqlContext.sql("select count(*) 总票数 from ticket_selling");
        countDF.show();
        //非公共账号数
        DataFrame statusNoPublic = sqlContext.sql("select count(*) 支付数量 from ticket_selling where status = 'ispay'");
        statusNoPublic.show();
        //总支付次数
        DataFrame payNumCount = sqlContext.sql("select SUM(money) 买票金额 FROM ticket_selling");
        payNumCount.show();
        //时间
        DataFrame createTime = sqlContext.sql("select count(*) 时间 FROM ticket_selling where createTime<  '2018-09-01 00:00:01' ");
        createTime.show();
    }

    //初始化 DataFrame
    public DataFrame getAppUserDataFrame(String tableName) {
        //第一步得到 javaRDD<Row>
        JavaRDD<Row> rowRDD = TicketSellingSQL.rowRDD;
        //第二 动态构造DataFrame的元数据。
        //得到全是字符串类型的
// List<StructField> structFields = CommonUtil.getStringStructFieldList(TicketSelling.class);
        //得到和类类型一样的
        List<StructField> structFields = CommonUtil.getStructFieldList(TicketSelling.class);
        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);
        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         */
        DataFrame personsDF = sqlContext.createDataFrame(rowRDD, structType);
        /**
         * 第四步：将数据写入到表中
         */
        personsDF.registerTempTable(tableName);
        return personsDF;
    }
}
