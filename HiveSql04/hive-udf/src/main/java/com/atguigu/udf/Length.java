package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/18
 */
public class Length extends GenericUDF {

    /**
     * 对输入的方法做检查，以及约束输出类型
     * @param arguments 输入参数检查器
     * @return 输出参数检查器
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("自定义函数输入参数长度有误");
        }

        if(! arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0,"自定义函数输入参数类型有误");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 实现逻辑方法
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if(o == null) {
            return 0;
        }
        return o.toString().length();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
