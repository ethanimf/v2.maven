/**
 * 
 */
package ethan.v2.ch12;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.DoubleWritable;

/**
 * @since 2013-10-17
 * @author ethan
 * @version $Id$
 */

public class Mean extends UDAF {

	  public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {
	    public static class PartialResult {
	      double sum;
	      long count;
	    }
	    
	    private PartialResult partial;

	    public void init() {
	      partial = null;
	    }

	    public boolean iterate(DoubleWritable value) {
	      if (value == null) {
	        return true;
	      }
	      if (partial == null) {
	        partial = new PartialResult();
	      }
	      partial.sum += value.get();
	      partial.count++;
	      return true;
	    }

	    public PartialResult terminatePartial() {
	      return partial;
	    }

	    public boolean merge(PartialResult other) {
	      if (other == null) {
	        return true;
	      }
	      if (partial == null) {
	        partial = new PartialResult();
	      }
	      partial.sum += other.sum;
	      partial.count += other.count;
	      return true;
	    }

	    public DoubleWritable terminate() {
	      if (partial == null) {
	        return null;
	      }
	      return new DoubleWritable(partial.sum / partial.count);
	    }
	  }
	}
