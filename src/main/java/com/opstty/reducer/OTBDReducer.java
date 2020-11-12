package com.opstty.reducer;

import com.opstty.WritableSub;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OTBDReducer extends Reducer<IntWritable, WritableSub, IntWritable, NullWritable>
{
    private int min = 2020;
    private int district = 0;

    public IntWritable oldest = new IntWritable();
    public void reduce(IntWritable key, Iterable<WritableSub> values, Context context) throws IOException, InterruptedException
    {

        for (WritableSub val : values)
        {
            if (min> val.getV2().get())
            {
                district = val.getV1().get();

                min = val.getV2().get();
            }
        }
        oldest.set(district);

        context.write(oldest, NullWritable.get());
    }
}