/**
Application Project 8:
Srinivasa Reddy Minukuri
Rajveer Sidhu
Sriharsha Reddy Mopuri
*/
import org.apache.commons.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class App 
{
    public static void main( String[] args) throws Exception
    {
    	long time=System.currentTimeMillis();
    	Configuration conf=new Configuration();
    	Job j1=Job.getInstance();
    	j1.setJobName("RankWords");
    	j1.setJarByClass(App.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);
    	j1.setMapperClass(Map1.class);
    	j1.setReducerClass(Reduce1.class);
        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1,new Path(args[1]+"\\Step1"));
        
//        MultipleOutputs.addNamedOutput(j1, e, outputFormatClass, keyClass, valueClass);
        j1.waitForCompletion(true);
    	
    	Configuration conf1=new Configuration();
    	Job j2=Job.getInstance();
    	j2.setJobName("Dominant");
    	j2.setJarByClass(App.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
    	j2.setMapperClass(LinearRegressionMap.class);
    	j2.setReducerClass(LinearRegressionReduce.class);
        j2.setInputFormatClass(KeyValueTextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(j2,new Path(args[1]+"/step1"));
        FileOutputFormat.setOutputPath(j2,new Path(args[1]+"/step2"));
        j2.waitForCompletion(true);

    	Configuration conf2=new Configuration();
    	Job j3=Job.getInstance();
    	j3.setJobName("SameRank");
    	j3.setJarByClass(App.class);
        j3.setOutputKeyClass(DoubleWritable.class);
        j3.setOutputValueClass(Text.class);
    	j3.setMapperClass(SortMap.class);
    	j3.setReducerClass(SortReduce.class);
    	j3.setSortComparatorClass(DecreasingComparator.class);
        j3.setInputFormatClass(KeyValueTextInputFormat.class);
        j3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(j3,new Path(args[1]+"/step2"));
        FileOutputFormat.setOutputPath(j3,new Path(args[1]+"/final"));
        j3.waitForCompletion(true);
    System.out.println("Execution time is : "+(System.currentTimeMillis()-time)*1000);
    }
}
