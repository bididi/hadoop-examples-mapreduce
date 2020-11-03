package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text tbskey, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = 0;

        for (IntWritable val : values) {
            if (val.get() > max) {
                max = val.get();
            }
        }
        result.set(max);
        context.write(tbskey, result);
    }
}