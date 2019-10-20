package com.archyuan.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    private Text phoneKey = new Text();
    private FlowBean flowBean = new FlowBean(0,0);
    Long up=0L;
    Long down=0L;
    Long sum=0L;
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

         for(FlowBean flowBean1:values){
             up += flowBean1.getUpFlow();
             down += flowBean1.getDownFlow();
             sum += flowBean1.getSumFlow();
         }

        flowBean.setSumFlow(sum);
        flowBean.setDownFlow(down);
        flowBean.setUpFlow(up);
        phoneKey.set(key);
        context.write(phoneKey,flowBean);
    }
}
