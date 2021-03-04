package com.lyc.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 16:16
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private Text outv = new Text();
    private FlowBean outk = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        outv.set(split[0]);
        outk.setUpFlow(Long.parseLong(split[1]));
        outk.setDownFlow(Long.parseLong(split[2]));
        outk.setSumFlow();
        context.write(outk, outv);
    }
}
