/**
 * 
 */
package ethan.v2.ch08;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.common.JobBuilder;
import ethan.v2.common.NcdcRecordParser;

/**
 * @since 2013-10-9
 * @author ethan
 * @version $Id$
 */

public class MaxTemperatureUsingSecondarySort_ethan extends Configured implements Tool {

	static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {

		NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				IntPair pair = new IntPair();
				pair.set(parser.getYearInt(), parser.getAirTemperature());
				context.write(pair, NullWritable.get());
			}
		}
	}

	static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {

		@Override
		protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

			context.write(key, NullWritable.get());
		}

	}

	static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {

		@Override
		public int getPartition(IntPair key, NullWritable value, int numPartitions) {

			return Math.abs(key.getFirst() * 127) % numPartitions;
		}

	}

	static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(IntPair.class);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			int l = ((IntPair) a).getSecond();
			int r = ((IntPair) b).getSecond();
			return l == r ? 0 : (l < r ? -1 : 1);
		}

	}

	static class groupComparator extends WritableComparator {

		protected groupComparator() {
			super(IntPair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			int l = ((IntPair) a).getFirst();
			int r = ((IntPair) b).getFirst();
			return l == r ? 0 : (l < r ? -1 : 1);
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(groupComparator.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new MaxTemperatureUsingSecondarySort_ethan(), args);
		System.exit(code);

	}
}
