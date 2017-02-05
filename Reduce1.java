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

import javax.print.DocFlavor.STRING;

import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.hash.Hash;

public class Reduce1 extends Reducer<Text, LongWritable, Text,LongWritable>{
MultipleOutputs<Text, Text> mos;
	    
public void setup(Context context) {
	mos = new MultipleOutputs(context);
	}

public void reduce(Text key,Iterable<LongWritable> Values, Context context) throws IOException,InterruptedException
	{
		long total=0;
		for(LongWritable item : Values)
		{
			total=total+item.get();
			
		}
		String[] info=key.toString().split("_");
//		context.write(key, new LongWritable(total));
		mos.write(new Text(info[0]),new Text(String.valueOf(total)),"/home/srinivas/Documents/out/step1/"+info[1]);
		
	}
protected void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
}
	}
