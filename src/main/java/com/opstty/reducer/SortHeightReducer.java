package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortHeightReducer extends Reducer<FloatWritable, NullWritable, FloatWritable, NullWritable>
{
    public void reduce(FloatWritable Key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        context.write(Key, NullWritable.get());
    }
}