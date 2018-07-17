package com.atguigu.mapreduce.reducejoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{
	
	
	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
			Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
		//准备存储订单集合
		ArrayList<TableBean> orderbeans = new ArrayList<>();
		//准备bean对象
		TableBean pdBean =new TableBean();
		
		for (TableBean oBean : values) {
			if ("0".equals(oBean.getFlag())) {//订单表
				//拷贝传递过来的每条数据到集合中
				TableBean Tbean =new TableBean();
				
				try {
					BeanUtils.copyProperties(Tbean, oBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				orderbeans.add(Tbean);
			}else{//产品表
				
				try {
					//拷贝传递过来的产品表到内存中
					BeanUtils.copyProperties(pdBean, oBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		//表的拼接
		for (TableBean tableBean : orderbeans) {
			tableBean.setpName(pdBean.getpName());
			
			//将数据写出去
			
			context.write(tableBean, NullWritable.get());
		}
		
		
	}
}
