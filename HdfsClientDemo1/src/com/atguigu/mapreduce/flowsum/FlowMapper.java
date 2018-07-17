package com.atguigu.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text>{
	
	FlowBean k =new FlowBean();
	Text v =new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//1 获取一行
		String lines = value.toString();
		
		//2 切割
		String[] fields = lines.split("\t");
		
		//3 封装 对象
		//手机号
		String phoneNum=fields[1];
		//上行流量
		long upFlow=Long.parseLong(fields[fields.length-3]);
		//下行流量
		long downFlow=Long.parseLong(fields[fields.length-2]);
		
		
		k.set(upFlow, downFlow);
		v.set(phoneNum);
		//4 写出数据
		context.write(k, v);
		
	}
}
