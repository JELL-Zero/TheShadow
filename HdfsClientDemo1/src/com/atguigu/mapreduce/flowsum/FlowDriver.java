package com.atguigu.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FlowDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1 获取job对象
		Configuration configuration =new Configuration();
		Job job =Job.getInstance(configuration);
		//2 指定本地jar包路径
		job.setJarByClass(FlowDriver.class);
		//3 指定要使用的mapper和reducer类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		//4设置mapper输出类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		
		//5 设置最终输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//设置分区
		job.setPartitionerClass(BelongPartitioner.class);
		job.setNumReduceTasks(5);
		
		
		//6设置输出输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//7 提交
		boolean result=job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
