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
		//׼���洢��������
		ArrayList<TableBean> orderbeans = new ArrayList<>();
		//׼��bean����
		TableBean pdBean =new TableBean();
		
		for (TableBean oBean : values) {
			if ("0".equals(oBean.getFlag())) {//������
				//�������ݹ�����ÿ�����ݵ�������
				TableBean Tbean =new TableBean();
				
				try {
					BeanUtils.copyProperties(Tbean, oBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				orderbeans.add(Tbean);
			}else{//��Ʒ��
				
				try {
					//�������ݹ����Ĳ�Ʒ���ڴ���
					BeanUtils.copyProperties(pdBean, oBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		//���ƴ��
		for (TableBean tableBean : orderbeans) {
			tableBean.setpName(pdBean.getpName());
			
			//������д��ȥ
			
			context.write(tableBean, NullWritable.get());
		}
		
		
	}
}
