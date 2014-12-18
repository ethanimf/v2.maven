package com.icenet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * ��ȡ HDFS��Ⱥ�����нڵ����
 * 
 */
public class TestGetHostNameInHDFS {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// ǿת�ɷֲ�ʽ�ļ�����
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		// ��ȡ�ڵ���Ϣ������
		DatanodeInfo[] dis = hdfs.getDataNodeStats();
		for (DatanodeInfo info : dis) {
			String name = info.getHostName();
			System.err.println(name);
		}
	}

}
