/**
 * 
 */
//package ethan.v2.ch08;
//
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.SequenceFile.CompressionType;
//import org.apache.hadoop.io.compress.GzipCodec;
//import org.apache.hadoop.mapred.MapFileOutputFormat;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import ethan.v2.common.JobBuilder;
//
///**
// * 
// * @since 2013-10-8
// * @author ethan
// * @version $Id$
// */
//public class SortByTemperatureToMapFile extends Configured implements Tool {
//
//	@Override
//	public int run(String[] args) throws Exception {
//		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
//		if (job == null) {
//			return -1;
//		}
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputKeyClass(IntWritable.class);
//		// job.setOutputValueClass(Text.class); //默认就是Text?
//		//1.1.2版本的API对第三版也不完全支持
//		job.setOutputFormatClass(MapFileOutputFormat.class);
//		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//
//		return job.waitForCompletion(true) ? 0 : 1;
//	}
//	
//	public static void main(String[] args) throws Exception{
//		System.exit(ToolRunner.run(new SortByTemperatureToMapFile(), args));
//	}

//}
