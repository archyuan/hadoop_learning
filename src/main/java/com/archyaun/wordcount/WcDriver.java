package com.archyaun.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取一个Job实例
        Job job = Job.getInstance(new Configuration());

        //2 设置类路径
        job.setJarByClass(WcDriver.class);
        //3 设置Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);
        //4设置Mapper和Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5 设置输入输出数据的路径

        FileInputFormat.setInputPaths(job, new Path("G:\\BigData_Result\\input\\hello.txt"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\BigData_Result\\output"));

        //6 提交job

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
