package com.lyc.mapreduce.wordwount.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 12:44
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //reduce阶段的输入key类型Text，其实也是mapper阶段的输出key类型
    //reduce阶段的输入value类型IntWritable，其实也就是mapper阶段的输出value类型
    //reduce阶段的输出key类型Text，程序最终输出
    //reduce阶段的输出value类型IntWritable，程序最终输出
    private int sum;
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        //示例：atguigu (1,1)
        //atguigu：key
        //(1,1):values
        sum = 0;
        //累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        //封装累加和
        outV.set(sum);
        //写出
        context.write(key, outV);
    }
}
