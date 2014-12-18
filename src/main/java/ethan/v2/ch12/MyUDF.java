package ethan.v2.ch12;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 
 */

/**
 * @since 2013-10-16
 * @author ethan
 * @version $Id$
 */

public class MyUDF extends UDF{
	private Text result = new Text();
	
	public Text evaluate(Text src){
		if(src == null) return null;
		result.set(StringUtils.strip(src.toString()));
		return result;
	}
	
	public Text evaluate(Text src,String stripChars){
		if(src == null) return null;
		result.set(StringUtils.strip(src.toString(),stripChars));
		return result;
	}
}
