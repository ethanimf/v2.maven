package com.icenet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * �г�hdfs�ļ�
 *
 */
public class TestListFilesInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		FileStatus[] fs = hdfs.listStatus(new Path("/test"));
		
		for(FileStatus f: fs){
			System.out.println("��ǰ�ļ���" + f.getPath());
		}
		
		hdfs.close();
	}
}
