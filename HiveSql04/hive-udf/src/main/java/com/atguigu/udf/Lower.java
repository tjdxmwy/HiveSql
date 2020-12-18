package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Description 这个是老API,新API参见com.atguigu.udf.Length
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/18
 */
public class Lower extends UDF {
    public String evaluate(String s) {
        if(s != null) {
            return s.toLowerCase();
        }
        return null;
    }
}
