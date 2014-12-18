package com.icenet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ɾ��hdfs�ļ���Ŀ¼
 *
 */
public class TestDeleteFileInHDFS {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		//����2��ʾ�ݹ�ɾ�����ɾ���ΪĿ¼���˲���Ҫ������true
		hdfs.delete(new Path("/test/a"), true);
		
		hdfs.close();
	}
}
