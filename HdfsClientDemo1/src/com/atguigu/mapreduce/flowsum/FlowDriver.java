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
		//1 ��ȡjob����
		Configuration configuration =new Configuration();
		Job job =Job.getInstance(configuration);
		//2 ָ������jar��·��
		job.setJarByClass(FlowDriver.class);
		//3 ָ��Ҫʹ�õ�mapper��reducer��
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		//4����mapper�������
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		
		//5 ���������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//���÷���
		job.setPartitionerClass(BelongPartitioner.class);
		job.setNumReduceTasks(5);
		
		
		//6����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//7 �ύ
		boolean result=job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
