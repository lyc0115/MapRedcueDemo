package com.lyc.mapreduce.patitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区案例演示：将统计结果按照手机归属地不同省份输出到不同文件中
 * 1.自定义类继承Partitioner，重写getPartition方法
 * 2.在job驱动中，设置自定义Partitioner
 * 3.自定义Partitioner后，要根据自定义Partitioner的逻辑设置相应的ReduceTask
 * @author lyc
 * @create 2021--03--03 21:24
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //text是手机号
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        //定义一个分区号变量partition，根据prePhone设置分区号
        int partition;
        if ("136".equals(prePhone)){
            partition = 0;
        }else if ("137".equals(prePhone)){
            partition = 1;
        }else if ("138".equals(prePhone)){
            partition = 2;
        }else if ("139".equals(prePhone)){
            partition = 3;
        }else{
            partition = 4;
        }
        return partition;
    }
}
