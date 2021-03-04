package com.lyc.mapreduce.writable.local;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参方法
 * 4.toString方法
 * @author lyc
 * @create 2021--03--03 16:01
 */
public class FlowBean implements Writable {
    private long upFlow; //上行流量
    private long downFlow; //下行流量
    private long sumFlow; //总流量

    //提供无参构造方法
    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.downFlow + this.upFlow;
    }

    //序列化方法，其中序列化方法和反序列化方法一定要保持一致
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.upFlow = dataInput.readLong();
         this.downFlow = dataInput.readLong();
         this.sumFlow = dataInput.readLong();
    }

    //重写tostring
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
