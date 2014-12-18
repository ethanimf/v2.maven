/**
 * 
 */
package ethan.v2.ch12;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * @since 2013-10-16
 * @author ethan
 * @version $Id$
 */

public class MaxNum extends UDAF {

	public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {
		private IntWritable result;

		@Override
		public void init() {
			System.err.printf("%s %s\n", hashCode(), "init");
			result = null;
		}

		public boolean iterate(IntWritable value) {
			System.err.printf("%s %s %s\n", hashCode(), "iterate", value);
			if (value == null) {
				return true;
			}
			if (result == null) {
				result = new IntWritable(value.get());
			} else {
				result.set(Math.max(result.get(), value.get()));
			}
			return true;
		}

		public IntWritable terminatePartial() {
			System.err.printf("%s %s\n", hashCode(), "terminatePartial");
			return result;
		}

		public boolean merge(IntWritable other) {
			System.err.printf("%s %s %s\n", hashCode(), "merge", other);
			return iterate(other);
		}

		public IntWritable terminate() {
			System.err.printf("%s %s\n", hashCode(), "terminate");
			return result;
		}
	}
}
