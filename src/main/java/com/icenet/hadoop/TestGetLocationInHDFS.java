package com.icenet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ����ĳ���ļ���HDFS��Ⱥ�е�λ��
 * 
 */
public class TestGetLocationInHDFS {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// ������һ��������ļ�
		Path path = new Path("/test/wordcount.txt");
		// �ļ�״̬
		FileStatus fileStatus = fs.getFileStatus(path);
		// �ļ���
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus,
				0, fileStatus.getLen());
		int blockLen = blockLocations.length;
		//System.err.println(blockLen);
		for (int i = 0; i < blockLen; i++) {
			// ������
			String[] hosts = blockLocations[i].getHosts();
			for (String host : hosts) {
				System.err.println(host);
			}
		}
	}

}
