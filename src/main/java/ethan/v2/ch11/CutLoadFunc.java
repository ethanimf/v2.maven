/**
 * 
 */
package ethan.v2.ch11;

//import java.io.IOException;
//import java.util.List;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputFormat;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.pig.LoadFunc;
//import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
//import org.apache.pig.data.Tuple;
//import org.apache.pig.data.TupleFactory;
//import org.w3c.dom.ranges.Range;

/**
 * @since 2013-10-13
 * @author ethan
 * @version $Id$
 */

//public class CutLoadFunc extends LoadFunc {
//	private List<Range> ranges;
//	private RecordReader reader;
//	private TupleFactory factory;
//
//	@Override
//	public InputFormat getInputFormat() throws IOException {
//
//		return new TextInputFormat();
//	}
//
//	@Override
//	public Tuple getNext() throws IOException {
//		try {
//			if (!reader.nextKeyValue()) {
//				return null;
//			}
//			Text value = (Text) reader.getCurrentValue();
//			Tuple tuple = factory.newTuple(size);
//
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	@Override
//	public void prepareToRead(RecordReader reader, PigSplit arg1) throws IOException {
//		this.reader = reader;
//	}
//
//	@Override
//	public void setLocation(String path, Job job) throws IOException {
//		FileInputFormat.setInputPaths(job, path);
//	}
//
//	public CutLoadFunc(String cuttingpattern){
//		Range
//	}
//}

//cc CutLoadFunc A LoadFunc UDF to load tuple fields as column ranges

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

//vv CutLoadFunc
public class CutLoadFunc extends LoadFunc {

private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

private final List<Range> ranges;
private final TupleFactory tupleFactory = TupleFactory.getInstance();
private RecordReader reader;

public CutLoadFunc(String cutPattern) {
  ranges = Range.parse(cutPattern);
}

@Override
public void setLocation(String location, Job job)
    throws IOException {
  FileInputFormat.setInputPaths(job, location);
}

@Override
public InputFormat getInputFormat() {
  return new TextInputFormat();
}

@Override
public void prepareToRead(RecordReader reader, PigSplit split) {
  this.reader = reader;
}

@Override
public Tuple getNext() throws IOException {
  try {
    if (!reader.nextKeyValue()) {
      return null;
    }
    Text value = (Text) reader.getCurrentValue();
    String line = value.toString();
    Tuple tuple = tupleFactory.newTuple(ranges.size());
    for (int i = 0; i < ranges.size(); i++) {
      Range range = ranges.get(i);
      if (range.getEnd() > line.length()) {
        LOG.warn(String.format(
            "Range end (%s) is longer than line length (%s)",
            range.getEnd(), line.length()));
        continue;
      }
      tuple.set(i, new DataByteArray(range.getSubstring(line)));
    }
    return tuple;
  } catch (InterruptedException e) {
    throw new ExecException(e);
  }
}
}
//^^ CutLoadFunc

