package com.opstty.mapper;

import com.opstty.WritableSub;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NTBDMapper2 extends Mapper<Object, Text, NullWritable, WritableSub>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        WritableSub dist_tree = new WritableSub();

        String[] ligne = value.toString().split("\t");

            String dis =ligne[0];
            String trees =ligne[1];


            IntWritable district = new IntWritable(Integer.parseInt(dis));
            IntWritable tree = new IntWritable(Integer.parseInt(trees));
            dist_tree.setV1(district);
            dist_tree.setV2(tree);
            context.write(NullWritable.get(), dist_tree);

        }
    }
