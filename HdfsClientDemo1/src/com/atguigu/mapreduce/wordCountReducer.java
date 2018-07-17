package com.atguigu.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class wordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		//1 ���㵥�ʸ���
		int sum=0;
		for (IntWritable count :values ) {
			sum+=count.get();
		}
		//2 ���
		context.write(key,new IntWritable(sum));
	}
}
