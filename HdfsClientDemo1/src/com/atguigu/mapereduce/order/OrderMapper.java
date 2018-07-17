package com.atguigu.mapereduce.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	
	OrderBean k=new OrderBean();
	@Override
	protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		//1 获取每一行
		String line = value.toString();
		//2 切割
		String[] fields = line.split("\t");
		//3 获取数据
		k.setOrder_id(Integer.parseInt(fields[0]));
		k.setPrice(Double.parseDouble(fields[2]));
		//4 写出
		context.write(k, NullWritable.get());
	}
	
}
