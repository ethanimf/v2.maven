/**
 * 
 */
package ethan.v2.ch07;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.common.JobBuilder;

//import ethan.v2.common.JobBuilder;

/**
 * 
 * @since 2013-10-6
 * @author bkw
 * @version $Id$
 *
 */

public class SmallFilesToSequenceFileCoverter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		job.getConfiguration().set("mapred.job.tracker", "u3:9001");
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(SequenFileMapper.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class SequenFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

		Text filekeyName = null;

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 写map输出
			context.write(filekeyName, value);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filekeyName = new Text(path.toString());
		}

	}
	
	public static void main(String[] args) throws Exception{
		int code = ToolRunner.run(new SmallFilesToSequenceFileCoverter(), args);
		System.exit(code);
		
	}

}
