package com.atguigu.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class wordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k=new Text();
	IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//ÿ������ת����String
		String line = value.toString();
		//��Ƭ
		String[] words = line.split(" ");
		//ѭ��д������һ�׶�
		for (String word : words) {
			k.set(word);
			context.write(k, v);
		}
	}
}
