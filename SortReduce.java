/**
Application Project 8:
Srinivasa Reddy Minukuri
Rajveer Sidhu
Sriharsha Reddy Mopuri
*/

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

public class SortReduce extends Reducer<DoubleWritable, Text, DoubleWritable,Text>{
	public void reduce(DoubleWritable key,Iterable<Text> Values, Context context) throws IOException,InterruptedException
{
		for(Text country : Values)
		{
			
			context.write(key,country);			
		}
}
}
