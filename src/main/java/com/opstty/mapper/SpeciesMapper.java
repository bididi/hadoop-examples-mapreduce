package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(!value.toString().contains("ESPECE")){
            Text espece = new Text(value.toString().split(";")[4]);
            context.write(espece,NullWritable.get());
        }
    }
}
