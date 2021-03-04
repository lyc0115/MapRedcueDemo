package com.lyc.mapreduce.writableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 16:30
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        //遍历values集合，循环写出，避免总流量相同的情况
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
