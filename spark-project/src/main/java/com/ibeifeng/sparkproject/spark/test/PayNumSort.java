package com.ibeifeng.sparkproject.spark.test;

import java.io.Serializable;
import java.util.Comparator;

public class PayNumSort implements Comparator<String> ,Serializable{
    @Override
    public int compare(String o1, String o2) {
        String s1 = o1.replaceAll("\"", "");
        String s2 = o2.replaceAll("\"", "");
        int i = Integer.parseInt(s1) - Integer.parseInt(s2);
//        System.out.println(s1+"-"+s2+"=" + i);
        return i;
    }
}
