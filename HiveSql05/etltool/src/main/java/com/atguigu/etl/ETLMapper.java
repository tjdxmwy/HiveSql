package com.atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author Mr.Horse
 * @Version v1.0.0
 * @Since 1.0
 * @Date 2020/12/18
 */
public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

    private Counter pass;
    private Counter fail;
    private StringBuilder sb = new StringBuilder();
    private Text result = new Text();

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        pass = context.getCounter("ETL", "Pass");
        fail = context.getCounter("ETL", "Fail");
    }

    /**
     * 将一行日志数据进行处理，字段不够的删除，第四个字段空格去掉，最后的字段用&连接
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //切分一行数据
        String[] line = value.toString().split("\t");

        //判断字段长度够不够
        if(line.length >= 9) {
            // 1.第四个字段空格去掉
            line[3] = line[3].replace(" ", "");
            // 2.最后的字段用&连接
            sb.setLength(0);
            for(int i = 0; i < line.length; i++) {
                //2.1 如果拼接的是最后一个字段
                if(i == line.length - 1) {
                    sb.append(line[i]);
                } else if(i <= 8) {
                    //2.2 如果拼接的是前9个字段
                    sb.append(line[i]).append("\t");
                } else {
                    //2.3 拼接的是第9个字段用&连接
                    sb.append(line[i]).append("&");
                }
            }

            result.set(sb.toString());
            context.write(result, NullWritable.get());
            pass.increment(1);
        } else {
            //数据不要了
            fail.increment(1);
        }

    }
}
