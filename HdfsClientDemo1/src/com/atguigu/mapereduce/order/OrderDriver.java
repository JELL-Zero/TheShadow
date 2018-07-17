package com.atguigu.mapereduce.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1 获取job对象
		Configuration configuration= new Configuration();
		Job job = Job.getInstance(configuration);
		
		//2 设置jar包加载路径
		job.setJarByClass(OrderDriver.class);
		
		//3 设置mapper和reducer处理类
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		
		//4 设置maper输出类型
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//5 设置最终输出类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//6 设置输入文件和输出文件的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//		//7 加载Grouping类
//		job.setGroupingComparatorClass(OrderGroupingComparator.class);
		
		//8 设置分区
		job.setPartitionerClass(OrderPartitioner.class);
		
		//9 设置reducetask 个数
		job.setNumReduceTasks(3);
		
		//10 提交
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
		
	}
}
