package ethan.v2.ch07;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.common.JobBuilder;

public class MinimalMapReduceWithDefaults extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
//		NLineInputFormat.setNumLinesPerSplit(job, 2000);
//		job.setInputFormatClass(NLineInputFormat.class);
//		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Mapper.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(HashPartitioner.class);
		//在数据量少的情况下，把tasks设置太大会影响效率。
		job.setNumReduceTasks(1);
		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
		System.exit(exitCode);
	}

}
