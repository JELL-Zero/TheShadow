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
		//1 ��ȡjob����
		Configuration configuration =new Configuration();
		Job job = Job.getInstance(configuration);
		//2 ����jar��·��
		job.setJarByClass(SequenceDriver.class);
		//3 ����Mapper��Reducer������
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(SequenceFileReducer.class);
		//4 �������������inputformat��outputformat
		job.setInputFormatClass(WholeFileInputformat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//5����mapper�����������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		//6 ���������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		//7 �����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//8 �ύ
		boolean result = job.waitForCompletion(true);
		
		System.exit(result?0:1);
	}
}
