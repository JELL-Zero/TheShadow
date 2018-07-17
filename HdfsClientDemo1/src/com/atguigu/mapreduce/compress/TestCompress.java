package com.atguigu.mapreduce.compress;

import java.io.File;
import java.io.FileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {
	public static void main(String[] args) throws IOException, Exception {
		// 1 测试压缩
		// compress("e:/input/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
		decompress("e:/input/hello.txt.bz2");
	}

	// 解压缩
	private static void decompress(String filename) throws Exception, IOException {
		// 0 校验
		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

		CompressionCodec codec = factory.getCodec(new Path(filename));
		
		if (codec == null) {
			System.out.println("不支持该解码器"+filename);
			return;
		}
		// 1 获取输入流、
		CompressionInputStream cis=codec.createInputStream(new FileInputStream(new File(filename)));
		
		// 2 获取输出流
		FileOutputStream fos = new FileOutputStream(new File(filename+"decode"));
		
		// 3 流的对考
		IOUtils.copyBytes(cis, fos, 1024*1024*5,false);
		
		// 4 关闭资源
		cis.close();
		fos.close();
		
	}

	private static void compress(String filename, String method) throws IOException, ClassNotFoundException {
		// 1 获取输入流
		FileInputStream fis = new FileInputStream(new File(filename));

		Class classname = Class.forName(method);
		@SuppressWarnings("unchecked")
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classname, new Configuration());
		// 2 获取输出流
		FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));

		CompressionOutputStream cos = codec.createOutputStream(fos);
		// 3 流的对拷
		IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);
		// 4 关闭资源
		fis.close();
		cos.close();
		fos.close();
	}
}
