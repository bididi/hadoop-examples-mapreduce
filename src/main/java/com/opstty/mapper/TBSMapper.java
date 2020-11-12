package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class TBSMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(!value.toString().contains("ESPECE")){
            Text dt = new Text(value.toString().split(";")[3]);
            context.write(dt,one);
        }
    }
}