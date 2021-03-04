package com.lyc.mapreduce.writable.local;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 16:30
 */
public class FlowReducer extends Reducer<org.apache.hadoop.io.Text, FlowBean, org.apache.hadoop.io.Text, FlowBean> {
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException,
            InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        //1.遍历values，将其中的上行流量和下行流量分别累加
        for (FlowBean flowBean : values) {
            totalUp += flowBean.getUpFlow();
            totalDown += flowBean.getDownFlow();
        }
        //2.封装outK和outV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //3.写出outK，outV
        context.write(key, outV);
    }
}
