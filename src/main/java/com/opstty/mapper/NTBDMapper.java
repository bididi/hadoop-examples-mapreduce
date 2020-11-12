package com.opstty.mapper;

import com.opstty.WritableSub;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NTBDMapper extends Mapper<Object, Text, IntWritable, WritableSub>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        WritableSub dist_year = new WritableSub();
        IntWritable one = new IntWritable(1);

        if(!value.toString().contains("ANNEE PLANTATION")) // ignore the first line
        {
            String dis = value.toString().split(";")[1];
            String ye = value.toString().split(";")[5];

            if (!ye.isEmpty())
            {
                IntWritable year = new IntWritable(Integer.parseInt(ye));
                IntWritable district = new IntWritable(Integer.parseInt(dis));
                dist_year.setV1(district);
                dist_year.setV2(year);
                context.write(one, dist_year);
            }
        }
    }
}