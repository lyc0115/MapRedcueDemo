package com.lyc.mapreduce.wordwount.local;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 12:42
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //方法参数解读
    //map阶段输入的key类型LongWritable：每行的偏移量
    //map阶段输入的value类型Text：相当于java中的string
    //map阶段输出的key类型Text
    //map阶段输出的value类型Intwritable
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //wordcount程序mapper阶段的主要任务是依次统计每个单词的频率，不做总统计
        //value代表拿到每行的内容，context作为联系mapper阶段和reduce阶段的桥梁
        //1.获取一行
        String line = value.toString();
        //2.切割，根据文本内容的格式
        String[] words = line.split(" ");
        //3.循环写出
        for (String word : words) {
            //由于context类中的write方法需要一个Text类型对象，故先将string类型的word转换为Text类型
            //由于每遍历一行（一行可能有多个单词）和map方法会遍历多行，故应将outK升级为类中属性，减少开辟内存空间
            //封装outK
            outK.set(word);
            //写出
            context.write(outK,outV);
        }
    }
}
