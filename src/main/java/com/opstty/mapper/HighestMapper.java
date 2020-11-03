package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(!value.toString().contains("ESPECE")){
            Text dt = new Text(value.toString().split(";")[4]);
            Text taille = new Text(value.toString().split(";")[7]);
            IntWritable one = new IntWritable((int) Long.parseLong(String.valueOf(taille)));
            context.write(dt,one);
        }
    }
}