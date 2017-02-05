/**
Application Project 8:
Srinivasa Reddy Minukuri
Rajveer Sidhu
Sriharsha Reddy Mopuri
*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;

public class SortMap extends Mapper<Text, Text, DoubleWritable,Text>{
	public void map(Text key,Text value,Context context) throws IOException,InterruptedException 
	{	
		String[] Country=key.toString().split("-");
		context.write(new DoubleWritable(new Double(value.toString())), new Text(Country[0]));
	}
	}
