package com.lyc.mapreduce.writable.hdfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 16:16
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行数据，转换成字符串
        String line = value.toString();
        //2.切割数据，根据内容的格式由“\t”切割
        String[] split = line.split("\t");
        //3.抽取我们需要的数据，手机号，上行流量，下行流量
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        //4.封装成outK，outV
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        //5.写出outK，outV
        context.write(outK, outV);
    }
}
