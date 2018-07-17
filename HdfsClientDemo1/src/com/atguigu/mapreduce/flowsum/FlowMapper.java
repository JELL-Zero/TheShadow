package com.atguigu.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean,Text>{
	
	FlowBean k =new FlowBean();
	Text v =new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//1 ��ȡһ��
		String lines = value.toString();
		
		//2 �и�
		String[] fields = lines.split("\t");
		
		//3 ��װ ����
		//�ֻ���
		String phoneNum=fields[1];
		//��������
		long upFlow=Long.parseLong(fields[fields.length-3]);
		//��������
		long downFlow=Long.parseLong(fields[fields.length-2]);
		
		
		k.set(upFlow, downFlow);
		v.set(phoneNum);
		//4 д������
		context.write(k, v);
		
	}
}
