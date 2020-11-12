package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<Object, Text, FloatWritable, NullWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        if(!value.toString().contains("HAUTEUR"))
        {
            String hauteur = value.toString().split(";")[6];
            if (!hauteur.isEmpty())
            {
                FloatWritable height = new FloatWritable(Float.parseFloat(hauteur));
                context.write(height,NullWritable.get());
            }
        }
    }
}
