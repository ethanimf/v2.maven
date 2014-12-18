package com.icenet.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCount {
	public static class MyMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);// ������int����
		private Text word = new Text(); // ��������String����

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.err.println(key + "," + value);
			// Ĭ������¼���ݿո�ָ��ַ�
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		};
	}

	// Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.err.println(key + "," + values);
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			;
			context.write(key, result);// ���������
		};
	}

	public static void main(String[] args) throws Exception {
		// ����������Ϣ
		Configuration conf = new Configuration();
		// ����Job
		Job job = new Job(conf, "Word Count");
		// ���ù�����
		job.setJarByClass(MyWordCount.class);
		// ����mapper��
		job.setMapperClass(MyMapper.class);
		// ��ѡ
		job.setCombinerClass(MyReducer.class);
		// ���úϲ�������
		job.setReducerClass(MyReducer.class);
		// ����keyΪString����
		job.setOutputKeyClass(Text.class);
		// ����valueΪint����
		job.setOutputValueClass(IntWritable.class);
		// ���û��ǽ����������
		FileInputFormat.setInputPaths(job, new Path("/user/hadoop/a.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/ra"));
		// ִ��
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
