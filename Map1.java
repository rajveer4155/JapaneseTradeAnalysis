/**
Application Project 8:
Srinivasa Reddy Minukuri
Rajveer Sidhu
Sriharsha Reddy Mopuri
*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;

public class Map1 extends Mapper<LongWritable, Text, Text,LongWritable>{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException 
	{	
		    String[] Columns=value.toString().split(",");
			String year=Columns[1];
			String Country=Columns[2];
			String trade=Columns[7];
			if(!year.contains("Year"))
			{
			context.write(new Text(year+"_"+Country),new LongWritable(Long.parseLong(trade)));
			}
			}

}
