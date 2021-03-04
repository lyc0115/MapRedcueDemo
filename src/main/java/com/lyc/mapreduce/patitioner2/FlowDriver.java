package com.lyc.mapreduce.patitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--03--03 16:38
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //指定自定义分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //同时指定相应数量的ReduceTask
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job, new Path("E:\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\outputflow1"));
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
