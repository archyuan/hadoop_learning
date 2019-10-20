package com.archyuan.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private Text phoneKey = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        phoneKey.set(fields[1]);
        flowBean.setDownFlow(Long.parseLong(fields[fields.length-2]));
        flowBean.setUpFlow(Long.parseLong(fields[fields.length-3]));
        flowBean.setSumFlow(Long.parseLong(fields[fields.length-2])+Long.parseLong(fields[fields.length-3]));
        context.write(phoneKey,flowBean);
    }
}
