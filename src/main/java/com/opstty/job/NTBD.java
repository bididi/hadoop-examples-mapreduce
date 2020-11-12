package com.opstty.job;


import com.opstty.WritableSub;
import com.opstty.mapper.NTBDMapper;
import com.opstty.mapper.NTBDMapper2;
import com.opstty.reducer.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class NTBD {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ntbd <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "ntbd");
        job.setJarByClass(com.opstty.job.NTBD.class);
        job.setMapperClass(NTBDMapper.class);
        job.setReducerClass(TBSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1], "Job"));
        job.waitForCompletion(true);


        Job job1 = Job.getInstance(conf, "MAX");
        job1.setJarByClass(com.opstty.job.NTBD.class);
        job1.setMapperClass(NTBDMapper2.class);
        job1.setReducerClass(NTBDReducer2.class);
        job1.setMapOutputKeyClass(NullWritable.class);
        job1.setMapOutputValueClass(WritableSub.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job1, new Path(otherArgs[otherArgs.length - 1], "Job"));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1], "Job1"));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}