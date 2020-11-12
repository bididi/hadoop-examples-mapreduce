package com.opstty.reducer;

import com.opstty.WritableSub;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OTBDReducer extends Reducer<IntWritable, WritableSub, IntWritable, NullWritable>
{
    public IntWritable oldest = new IntWritable();
    public void reduce(IntWritable oneForAll, Iterable<WritableSub> values, Context context) throws IOException, InterruptedException
    {
        int minYear = 2020;
        int district = 0;
        for (WritableSub val : values)
        {
            if (minYear > val.getV2().get())
            {
                minYear = val.getV2().get();
                district = val.getV1().get();
            }
        }
        oldest.set(district);

        context.write(oldest, NullWritable.get());
    }
}