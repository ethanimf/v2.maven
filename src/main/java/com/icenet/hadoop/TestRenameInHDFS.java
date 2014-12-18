package com.icenet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ������hdfs�ļ�(�ƶ��ļ�)
 *
 */
public class TestRenameInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		hdfs.rename(new Path("/test/itcast.txt"), new Path("/var/hadoop.txt"));
		
		hdfs.close();
	}
}
