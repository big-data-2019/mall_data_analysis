package com.ibeifeng.sparkproject.spark.test.utils;

import com.ibeifeng.sparkproject.spark.test.pojo.AppUser2;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class CommonUtil {
    public static FastDateFormat fdf = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss");

    //根据类的类别得到 元数据描述对象
    public static List<StructField> getStructFieldList(Class clazz) {
        List<StructField> structFields = new ArrayList();
        Field[] fields = clazz.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            //类型
            String type = field.getType().getName();
            switch (type) {
                case "java.lang.String":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.StringType, true));
                    break;
                case "java.lang.Boolean":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.BooleanType, true));
                    break;
                case "java.lang.Integer":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.IntegerType, true));
                    break;
                case "java.sql.Timestamp"://1.5版本时间类型只支持timestamp 时间格式 yyyy-MM-dd HH:mm:ss格式
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.TimestampType, true));
                    break;
                case "java.lang.Long":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.LongType, true));
                    break;
                case "java.lang.Double":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.DoubleType, true));
                    break;
            }
        }
        return structFields;
    }

    //得到字符串的元数据描述对象
    public static List<StructField> getStringStructFieldList(Class clazz) {
        List<StructField> structFields = new ArrayList();
        Field[] fields = clazz.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            structFields.add(DataTypes.createStructField(field.getName(), DataTypes.StringType, true));
        }
        return structFields;
    }

    //根据 类的类型 得到Row对印的数据类型数据的 数组
    public static Object[] getRowArrByClazz(Object[] splited, Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            //类型
            String type = field.getType().getName();
            switch (type) {
                case "java.lang.Integer":
                    try {
                        objects[i] = Integer.parseInt(splited[i].toString());
                    } catch (Exception e) {
                        objects[i] = 0;
                    }
                    break;
                case "int":
                    try {
                        objects[i] = Integer.parseInt(splited[i].toString());
                    } catch (Exception e) {
                        objects[i] = 0;
                    }
                    break;
                case "java.sql.Timestamp":
                    try {
                        Timestamp timestamp = Timestamp.valueOf(splited[i].toString().replaceAll("/", "-"));
                        objects[i] = timestamp;
                    } catch (Exception e) {
                        objects[i] = null;
                    }
                    break;
                case "java.lang.Long":
                    try {
                        objects[i] = Long.parseLong(splited[i].toString());
                    } catch (Exception e) {
                        objects[i] = 0L;
                    }
                    break;
                case "long":
                    try {
                        objects[i] = Long.parseLong(splited[i].toString());
                    } catch (Exception e) {
                        objects[i] = 0L;
                    }
                    break;
                case "java.lang.Double":
                    try {
                        objects[i] = Double.parseDouble(splited[i].toString());

                    } catch (Exception e) {
                        objects[i] = 0d;
                    }
                    break;
                case "double":
                    try {
                        objects[i] = Double.parseDouble(splited[i].toString());

                    } catch (Exception e) {
                        objects[i] = 0d;
                    }
                    break;
                case "java.lang.Boolean":
                    try {
                        objects[i] = Boolean.parseBoolean(splited[i].toString());
                    } catch (Exception e) {
                        objects[i] = true;
                    }
                    break;
                case "boolean":
                    try {
                        objects[i] = Boolean.parseBoolean(splited[i].toString());
                    } catch (Exception e) {
                        objects[i] = true;
                    }
                    break;
                default:
                    try {
                        objects[i] = splited[i].toString();
                    } catch (Exception e) {
                        objects[i] = null;
                    }
                    break;
            }
        }
        return objects;
    }

    public static void main(String[] args) {
        String s = "83,2016/8/29 18:28:19,1,,2019/1/7 16:38:50,/headPortrait/83/d833c895d143ad4bd74bac3f85025aafa50f06e3.jpg,,冲哥哥,4PHwE7hpihMXTtWJTlLayPIXghBn7LFEt6yRRJ/Qh13NPLTgSnmxfrMxnq5P5CBZud6WBd8Skd7t4B/TaMgMEw==,1,ee307b42ced04b37b1679149ec9bffcf,2731,84,,07591432,45,formal,21,,";
        String[] split = s.split(",");
        Object[] arr = getRowArrByClazz(split, AppUser2.class);
        SparkUtil.printArray(arr);
        List<StructField> structFieldList = getStructFieldList(AppUser2.class);
        System.out.println("structFieldList = " + structFieldList);
        
       /* Object[] objects = new Object[]{"2"};
        long l = Long.parseLong("83");
        objects[0] = l;
        System.out.println(Arrays.toString(objects));*/
    }
}
