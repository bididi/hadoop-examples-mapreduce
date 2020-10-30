package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class DtMapper extends Mapper<Object, Text, Text, NullWritable> {
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(!value.toString().contains("ARRONDISSEMENT")){
            Text dt = new Text(value.toString().split(";")[1]);
            context.write(dt,NullWritable.get());
        }
    }
}
