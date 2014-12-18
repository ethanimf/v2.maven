/**
 * 
 */
package ethan.v2.ch08;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.common.JobBuilder;

/**
 * @since 2013-10-9
 * @author ethan
 * @version $Id$
 * 利用TotalOrderPartitioner根据intWritable键对顺序文件进行全局排序
 * 全局排序
 */

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
		job.setPartitionerClass(TotalOrderPartitioner.class);

		job.setNumReduceTasks(10);
		// freq Probability with which a key will be chosen.
		// numSamples Total number of samples to obtain from all selected
		// splits.
		// maxSplitsSampled The maximum number of splits to examine.
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);

		// Add to DistributedCache
		Configuration conf = job.getConfiguration();
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI uri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(uri, conf);
		DistributedCache.createSymlink(conf);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception{
		int code = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(code);
	}
}
