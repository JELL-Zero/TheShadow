package com.atguigu.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable>{

	private Configuration configuration;
	private FileSplit split;
	private boolean processed = false;
	private BytesWritable value = new BytesWritable();
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.split=(FileSplit) split;
		configuration=context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			byte[] buf=new byte[(int) split.getLength()];
			FileSystem fs = null;
			FSDataInputStream fis = null;
			try {
				Path path = split.getPath();
				fs=path.getFileSystem(configuration);
				
				fis=fs.open(path);
				
				IOUtils.readFully(fis, buf, 0, buf.length);
				
				value.set(buf,0,buf.length);
			} catch (Exception e) {
				
			}finally {
				IOUtils.closeStream(fis);
				IOUtils.closeStream(fs);
			}
			processed=true;
			return true;
		}
		
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		return processed?1:0;
	}

	@Override
	public void close() throws IOException {
		
		
	}

	
}
