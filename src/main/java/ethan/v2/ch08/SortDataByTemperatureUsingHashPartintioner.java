/**
 * 
 */
package ethan.v2.ch08;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.common.JobBuilder;

/**
 * @since 2013-10-8
 * @author ethan
 * @version $Id$
 * 调用默认的HashPartitioner根据Intwritable键对顺序文件进行排序
 * 部分排序
 */

public class SortDataByTemperatureUsingHashPartintioner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setCompressOutput(job, false);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new SortDataByTemperatureUsingHashPartintioner(), args);
		System.exit(code);
		
	}
}
