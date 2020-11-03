package com.opstty.job;

import com.opstty.mapper.DtMapper;
import com.opstty.mapper.HighestMapper;
import com.opstty.mapper.SpeciesMapper;
import com.opstty.mapper.TBSMapper;
import com.opstty.reducer.DtReducer;
import com.opstty.reducer.HighestReducer;
import com.opstty.reducer.SpeciesReducer;
import com.opstty.reducer.TBSReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Highest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: highest <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "highset");
        job.setJarByClass(com.opstty.job.Highest.class);
        job.setMapperClass(HighestMapper.class);
        job.setCombinerClass(HighestReducer.class);
        job.setReducerClass(HighestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}