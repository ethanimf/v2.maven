package ethan.v2.ch07;

import ethan.v2.common.JobBuilder;
import ethan.v2.common.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 
 * @since 2013-10-7
 * @author ethan
 * @version $Id$
 * 
 */
public class PartitionByStationUsingYearMultipleOutputs extends Configured implements Tool {

	static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			context.write(new Text(parser.getStationId()), value);
		}

	}

	static class StationReducer extends Reducer<Text, Text, NullWritable, Text> {
		
		private MultipleOutputs<NullWritable, Text> multipleout;
		private NcdcRecordParser parser = new NcdcRecordParser();
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleout.close();
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text value : values) {
				 String basePath = String.format("%s/%s/part",
				            parser.getStationId(), parser.getYear());
				multipleout.write(NullWritable.get(), value, basePath);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleout = new MultipleOutputs<NullWritable, Text>(context);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = JobBuilder.parseInputAndOutput(new PartitionByStationUsingYearMultipleOutputs(), getConf(), args);
		if (job == null) {
			return -1;
		}
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
		job.setMapperClass(StationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(StationReducer.class);
		job.setOutputKeyClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new PartitionByStationUsingYearMultipleOutputs(), args);
		System.exit(code);
	}
}
