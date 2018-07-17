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
		//1 ��ȡjob����
		Configuration configuration= new Configuration();
		Job job = Job.getInstance(configuration);
		
		//2 ����jar������·��
		job.setJarByClass(OrderDriver.class);
		
		//3 ����mapper��reducer������
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		
		//4 ����maper�������
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//5 ���������������
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//6 ���������ļ�������ļ���·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//		//7 ����Grouping��
//		job.setGroupingComparatorClass(OrderGroupingComparator.class);
		
		//8 ���÷���
		job.setPartitionerClass(OrderPartitioner.class);
		
		//9 ����reducetask ����
		job.setNumReduceTasks(3);
		
		//10 �ύ
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
		
	}
}
