/**
 * 
 */
package ethan.v2.ch13;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ethan.v2.common.NcdcRecordParser;

/**
 * @since 2013-10-18
 * @author ethan
 * @version $Id$
 */

public class HBaseTemperatureImporter extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: HBaseTemperatureImporter <input>");
			return -1;
		}

		JobConf jc = new JobConf(getConf(), getClass());
		FileInputFormat.addInputPath(jc, new Path(args[0]));
		jc.setMapperClass(HBaseTemperatureMapper.class);
		jc.setNumReduceTasks(0);
		jc.setOutputFormat(NullOutputFormat.class);
		JobClient.runJob(jc);
		return 0;
	}

	static class HBaseTemperatureMapper<K, V> extends MapReduceBase implements Mapper<LongWritable, Text, K, V> {

		private NcdcRecordParser parser = new NcdcRecordParser();
		private HTable table;

		@Override
		public void close() throws IOException {
			super.close();
			this.table.close();
		}

		@Override
		public void configure(JobConf job) {			
			super.configure(job);
			// Create the HBase table client once up-front and keep it around
			// rather than create on each map invocation.
			try {
				this.table = new HTable(new HBaseConfiguration(job), "observations");
			} catch (IOException e) {
				throw new RuntimeException("Failed HTable construction", e);
			}
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<K, V> output, Reporter reporter) throws IOException {

			parser.parse(value.toString());
			if (parser.isValidTemperature()) {
				byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(), parser.getObservationDate().getTime());
				Put p = new Put(rowKey);
				p.add(HBaseTemperatureCli.DATA_COLUMNFAMILY, HBaseTemperatureCli.AIRTEMP_QUALIFIER, Bytes.toBytes(parser.getAirTemperature()));
				table.put(p);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HBaseConfiguration(), new HBaseTemperatureImporter(), args);
		System.exit(exitCode);
	}
}
