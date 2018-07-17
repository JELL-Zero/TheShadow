package com.atguigu.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1 获取配置信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		// 2 设置 jar 加载路径
		job.setJarByClass(wordCountDriver.class);
		// 3 设置 map 和 Reduce 类
		job.setMapperClass(wordCountMapper.class);
		job.setReducerClass(wordCountReducer.class);
		// 4 设置 map 输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 5 设置最终 输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
		
	}
}
