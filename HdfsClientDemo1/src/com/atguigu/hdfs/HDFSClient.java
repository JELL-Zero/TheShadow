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
		// 1 �����ļ�
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		// 2 �ϴ��ļ�
		fs.copyFromLocalFile(new Path("e://wc.jar"), new Path("/wc.jar"));
		// 3 �ر���Դ'
		fs.close();
		System.out.println("over");
	}

	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 ִ�����ز���
		fs.copyToLocalFile(false, new Path("/hello.txt"), new Path("e://hello2.txt"), true);
		// 3 �ر���Դ
		fs.close();

	}

	@Test
	public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 �����ļ�Ŀ¼
		fs.mkdirs(new Path("/test/java"));

		// 3 �ر���Դ
		fs.close();
	}

	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		//2 ɾ���ļ�
		fs.delete(new Path("/test"),true);
		//3 �ر���Դ
		fs.close();
	}
	
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
		//1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"atguigu");
		//2 ��ȡ�ļ�����
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		//3 �ر���Դ
		while (listFiles.hasNext()) {
			LocatedFileStatus listFile = listFiles.next();
			//�������
			//�ļ�����
			System.out.println(listFile.getPath().getName());
			//����
			System.out.println(listFile.getLen());
			//Ȩ��
			System.out.println(listFile.getPermission());
			//��
			System.out.println(listFile.getGroup());
			//��ȡ�洢�Ŀ���Ϣ
			BlockLocation[] blockLocations = listFile.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				//��ȡ��������ڵ�
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("----------------------------------");
		}
		
	}

	@Test
	//HDFS�ļ��ϴ�
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException{
		//1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		//2 ����������
		FileInputStream fis = new FileInputStream(new File("e://hello.txt"));
		//3 ���������
		FSDataOutputStream fos = fs.create(new Path("/hello3.txt"));
		//4 �Կ���
		IOUtils.copyBytes(fis, fos,configuration);
		//5 �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
	
	@Test
	//�ļ�����
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
		//1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
		//2  ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/hello3.txt"));
		//3 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("e://hello3.txt"));
		//4 ���ĶԿ�
		
		IOUtils.copyBytes(fis,fos, configuration);
		//4 �ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		fs.close();
	}
}
