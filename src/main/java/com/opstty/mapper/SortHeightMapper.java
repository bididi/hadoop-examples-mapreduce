package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable id = new IntWritable();
    private Text height = new Text();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {



        String[] ligne = value.toString().split(";");
        if (!ligne[6].equals("HAUTEUR")&&!ligne[6].equals("")&&!ligne[11].equals("OBJECTID"))
            height.set(ligne[6]);
            id.set(Integer.parseInt(ligne[11]));


        context.write(height, id);
    }
}
