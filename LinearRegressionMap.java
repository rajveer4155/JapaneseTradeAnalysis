/**
Application Project 8:
Srinivasa Reddy Minukuri
Rajveer Sidhu
Sriharsha Reddy Mopuri
*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;

public class LinearRegressionMap extends Mapper<Text, Text, Text,Text>{
	
	int count=0;
    int MAXN = 1000;
    int n = 0;
    double[] x = new double[MAXN];
    double[] y = new double[MAXN];
    String filename="";
    double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;

	public void map(Text key,Text value,Context context) throws IOException,InterruptedException 
	{	
		if(count==0)
		{
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			filename = fileSplit.getPath().getName();
			count++;
		}
        // first pass: read in data, compute xbar and ybar
       
            x[n] = Double.parseDouble(key.toString());
            y[n] = Double.parseDouble(value.toString());
            sumx  += x[n];
            sumx2 += x[n] * x[n];
            sumy  += y[n];
            n++;
        
  	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		 double xbar = sumx / n;
	        double ybar = sumy / n;

	        // second pass: compute summary statistics
	        double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
	        for (int i = 0; i < n; i++) {
	            xxbar += (x[i] - xbar) * (x[i] - xbar);
	            yybar += (y[i] - ybar) * (y[i] - ybar);
	            xybar += (x[i] - xbar) * (y[i] - ybar);
	        }
	        double beta1 = xybar / xxbar;
	        double beta0 = ybar - beta1 * xbar;
	        context.write(new Text(filename), new Text(String.valueOf(beta0)+"_"+String.valueOf(beta1)));
	        System.err.println(beta0 + "equation" +beta1);
	}
	
	
	}


