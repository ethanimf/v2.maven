/**
 * 
 */
package com.icenet.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @since 2013-6-26
 * @author ethan
 * @version $Id$
 * 
 */

public class MacTest extends Configured implements Tool {
	// 计数器
	enum Counter {
		LINESKIP,
	}
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException {
			String line = value.toString();
			try {
				String[] linesplit = line.split(" ");
				String month= linesplit[0];
				String time  =  linesplit[1];
				String mac = linesplit[6];
				Text out = new Text(month+' '+time +' '+mac);
				context.write(NullWritable.get(), out);
				
			} catch (ArrayIndexOutOfBoundsException e) {
				// TODO: handle exception
				context.getCounter(Counter.LINESKIP).increment(1); //出错令计数器+1
				return;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
		Job job = new Job(conf,"ice");
		job.setJarByClass(MacTest.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); //输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //输出路径
		
		job.setMapperClass(Map.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);  //指定输出的key格式
		job.setOutputValueClass(Text.class); //指定输出的value格式
		
		job.waitForCompletion(true);
		
		System.out.println("任务名称："+job.getJobID());
		System.out.println("任务成功:"+(job.isSuccessful()?"是":"否"));
		System.out.println("输入行数:"+job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue());
		System.out.println( "输出行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue() );
		System.out.println("跳过的行："+job.getCounters().findCounter(Counter.LINESKIP).getValue());
		return job.isSuccessful() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length!=2) {
			System.err.println("USAGE:TESTMAC <input path> <output path>");
			System.exit(-1);
		}
		//记录开始时间
		Date start = new Date();
		int res = ToolRunner.run(new Configuration(), new MacTest(),args);
		//记录结束时间
		Date end = new Date();
		float time = (float)((end.getTime()-start.getTime())/1000.0);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("任务开始时间："+df.format(start));
		System.out.println("任务结束时间："+df.format(end));
		System.out.println("任务执行时间："+time+"s");
		System.exit(res);
		
	}
}


