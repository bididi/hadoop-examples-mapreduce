package com.opstty.reducer;

import com.opstty.WritableSub;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NTBDReducer2 extends Reducer<NullWritable, WritableSub, IntWritable, IntWritable>
{

    public IntWritable resultat = new IntWritable();
    public IntWritable district1 = new IntWritable();
    public int district ;
    public int maxTree = 0;

    public void reduce(NullWritable key, Iterable<WritableSub> values, Context context) throws IOException, InterruptedException
    {

        for (WritableSub val : values)
        {
            if (maxTree < val.getV2().get())
            {
                district = val.getV1().get();

                maxTree = val.getV2().get();
            }
        }
        district1.set(district);
        resultat.set(maxTree);


        context.write(district1, resultat);
    }

}