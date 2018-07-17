package com.atguigu.mapreduce.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{
	TableBean Tbean= new TableBean();
	Text k =new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//��ȡ�ļ�����
		FileSplit split = (FileSplit) context.getInputSplit();
		String name = split.getPath().getName();
		//��ȡ��������
		String line = value.toString();
		
		//��ͬ�ļ��ֱ���
		if (name.startsWith("order")) {//������
			//�и�
			String[] fields = line.split("\t");
			//��װbean
			Tbean.setOrder_id(fields[0]);
			Tbean.setpId(fields[1]);
			Tbean.setAmount(Integer.parseInt (fields[2]));
			Tbean.setpName("");
			Tbean.setFlag("0");
			k.set(fields[1]);
		}else{//��Ʒ��
			String[] fields=line.split("\t");
			Tbean.setpId(fields[0]);
			Tbean.setpName(fields[1]);
			Tbean.setFlag("1");
			Tbean.setOrder_id("");
			Tbean.setAmount(0);
			k.set(fields[0]);
		}
		context.write(k, Tbean);
	}
}
