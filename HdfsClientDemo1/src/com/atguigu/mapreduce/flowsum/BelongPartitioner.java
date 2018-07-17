package com.atguigu.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BelongPartitioner extends Partitioner<FlowBean,Text>{
	

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitons) {
	String preNum = value.toString().substring(0,3);
		
		int partitions=4;
		
		if("136".equals(preNum)){
			partitions=0;
		}else if("137".equals(preNum)){
			partitions=1;
		}else if("138".equals(preNum)){
			partitions=2;
		}else if("139".equals(preNum)){
			partitions=3;
		}
			
		return partitions;
	}

}
