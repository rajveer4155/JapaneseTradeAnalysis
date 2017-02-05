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
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;
public class LinearRegressionReduce extends Reducer<Text, Text, Text,DoubleWritable>{
	public void reduce(Text key,Iterable<Text> Values, Context context) throws IOException,InterruptedException
{
		int count=0;
		String[] betas;
		Double predict=new Double(0);
	for(Text value:Values)
	{
		betas=value.toString().split("_");
		predict= Double.parseDouble(betas[1]);
		predict=predict*2016;
		predict=predict+Double.parseDouble(betas[0]);
	}
context.write(key,new DoubleWritable(predict));
}
}
