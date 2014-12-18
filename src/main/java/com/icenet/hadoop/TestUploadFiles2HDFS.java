package com.icenet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * �ϴ������ļ���Ŀ¼��HDFS
 *
 */
public class TestUploadFiles2HDFS {

	public static void main(String[] args) throws Exception {
		
		//��ʼ�����ö���
		Configuration conf = new Configuration();
		
		//���hdfs����
		FileSystem hdfs = FileSystem.get(conf);
		
		//�����ļ�
		Path p1 = new Path("c:\\hadoop");
		
		//Ŀ���ļ�
		Path p2 = new Path("/test/");
		
		//����ļ��ϴ�
		hdfs.copyFromLocalFile(p1, p2);
		
		System.out.println("�ļ��ϴ�����" + conf.get("fs.default.name"));
		hdfs.close();
	}

}
