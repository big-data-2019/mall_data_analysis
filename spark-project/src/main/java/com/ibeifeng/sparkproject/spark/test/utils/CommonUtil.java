package com.ibeifeng.sparkproject.spark.test.utils;

import com.ibeifeng.sparkproject.spark.test.pojo.Language;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class CommonUtil {
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
                case "java.lang.Integer":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.IntegerType, true));
                    break;
                case "java.util.Date":
                    structFields.add(DataTypes.createStructField(field.getName(), DataTypes.DateType, true));
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

    public static void main(String[] args) {
        List<StructField> structFieldList = getStructFieldList(Language.class);

    }
}
