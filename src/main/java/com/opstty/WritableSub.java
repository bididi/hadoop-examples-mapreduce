package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableSub implements Writable {

    private IntWritable v1 = new IntWritable();
    private IntWritable v2 = new IntWritable();
    public WritableSub(IntWritable d, IntWritable s){
        this.v1 = d;
        this.v2 = s;

    }
    public WritableSub() {
        this.v1 = new IntWritable(0);
        this.v2 = new IntWritable(0);

    }

    public void setV1(IntWritable v1) {
        this.v1 = v1;
    }

    public void setV2(IntWritable v2) {
        this.v2 = v2;
    }

    public IntWritable getV1() {
        return v1;
    }

    public IntWritable getV2() {
        return v2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        v1.write(dataOutput);
        v2.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        v1.readFields(dataInput);
        v2.readFields(dataInput);

    }
}
