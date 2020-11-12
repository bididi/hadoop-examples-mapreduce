package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static FloatWritable height = new FloatWritable();
    private Text espece = new Text();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {



        String[] ligne = value.toString().split(";");
        if (!ligne[6].equals("HAUTEUR")&&!ligne[6].equals("")&&!ligne[3].equals("ESPECE")) {
            height.set( Float.parseFloat(ligne[6]));
            espece.set(ligne[3]);
        }


        context.write(espece, height);
    }
}
