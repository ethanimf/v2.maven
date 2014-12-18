/**
 * 
 */
package ethan.v2.ch08;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.ch02.MaxTemperatureReducer;
import ethan.v2.common.JobBuilder;
import ethan.v2.common.NcdcRecordParser;

/**
 * @since 2013-10-8
 * @author ethan
 * @version $Id$
 * 
 */

public class MaxTemperatureWithCounters extends Configured implements Tool {

	enum Temperature {
		MISSING, MALFORMED
	}

	static class CoutersMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				int airTemperature = parser.getAirTemperature();
				context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
			} else if (parser.isMalformedTemperature()) {
				System.err.println("Ignoring possibly corrupt input: " + value);
				context.getCounter(Temperature.MALFORMED).increment(1);
			} else if (parser.isMissingTemperature()) {
				context.getCounter(Temperature.MISSING).increment(1);
			}

		}

	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = JobBuilder.parseInputAndOutput(new MaxTemperatureWithCounters(), getConf(), args);
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
		job.setOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setMapperClass(CoutersMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new MaxTemperatureWithCounters(), args);
		System.exit(code);
	}
}
