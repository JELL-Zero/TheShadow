package com.atguigu.mapereduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartition) {
		
		return (key.getOrder_id() & Integer.MAX_VALUE) % numPartition;
	}

}
