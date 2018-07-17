package com.atguigu.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class SequenceDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args=new String[]{"e:/input/input2","e:/output2"};
		//1 获取job对象
		Configuration configuration =new Configuration();
		Job job = Job.getInstance(configuration);
		//2 设置jar包路径
		job.setJarByClass(SequenceDriver.class);
		//3 设置Mapper和Reducer处理类
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(SequenceFileReducer.class);
		//4 设置输入输出的inputformat和outputformat
		job.setInputFormatClass(WholeFileInputformat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//5设置mapper输入输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		//6 设置最终输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		//7 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//8 提交
		boolean result = job.waitForCompletion(true);
		
		System.exit(result?0:1);
	}
}
