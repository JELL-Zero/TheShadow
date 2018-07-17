package com.atguigu.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;



public class HDFSClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		// 1 配置文件
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		// 2 上传文件
		fs.copyFromLocalFile(new Path("e://wc.jar"), new Path("/wc.jar"));
		// 3 关闭资源'
		fs.close();
		System.out.println("over");
	}

	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 执行下载操作
		fs.copyToLocalFile(false, new Path("/hello.txt"), new Path("e://hello2.txt"), true);
		// 3 关闭资源
		fs.close();

	}

	@Test
	public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 创建文件目录
		fs.mkdirs(new Path("/test/java"));

		// 3 关闭资源
		fs.close();
	}

	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		//2 删除文件
		fs.delete(new Path("/test"),true);
		//3 关闭资源
		fs.close();
	}
	
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
		//1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"atguigu");
		//2 获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		//3 关闭资源
		while (listFiles.hasNext()) {
			LocatedFileStatus listFile = listFiles.next();
			//输出详情
			//文件名称
			System.out.println(listFile.getPath().getName());
			//长度
			System.out.println(listFile.getLen());
			//权限
			System.out.println(listFile.getPermission());
			//组
			System.out.println(listFile.getGroup());
			//获取存储的块信息
			BlockLocation[] blockLocations = listFile.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				//获取块的主机节点
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("----------------------------------");
		}
		
	}

	@Test
	//HDFS文件上传
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException{
		//1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		//2 创建输入流
		FileInputStream fis = new FileInputStream(new File("e://hello.txt"));
		//3 创建输出流
		FSDataOutputStream fos = fs.create(new Path("/hello3.txt"));
		//4 对拷流
		IOUtils.copyBytes(fis, fos,configuration);
		//5 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	@Test
	//文件下载
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
		//1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		//2  获取输入流
		FSDataInputStream fis = fs.open(new Path("/hello3.txt"));
		//3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("e://hello3.txt"));
		//4 流的对拷
		
		IOUtils.copyBytes(fis,fos, configuration);
		//4 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
}
