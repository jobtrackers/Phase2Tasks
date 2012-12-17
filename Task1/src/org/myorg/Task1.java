package org.myorg;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Task1 {


	public static class Task1_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text template = new Text("template");
		private Text no_template = new Text("no_template");
		private Text total = new Text("Total pages with Infobox");
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			Pattern p1=Pattern.compile("(.*?)\\{\\{Infobox(\\|\\w*|l\\s[^\\w+])");
			Matcher m; 
			String line = value.toString();
			m = p1.matcher(line);
			if(m.find()){
				output.collect(no_template, one);
			}
			else{
				output.collect(template, one);
			}
			output.collect(total, one);
		}
	}

	public static class Task1_Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task1.class);
		conf.setJobName("task1");
		conf.setJarByClass(Task1_Map.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Task1_Map.class);
		conf.setCombinerClass(Task1_Reduce.class);
		conf.setReducerClass(Task1_Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		RunningJob runjob =JobClient.runJob(conf);
		if(runjob.isComplete()){
			String srcPath = args[1]; 
			String dstPath = "s3n://phase2mergedout/task1/";//"s3n://phase2mergedout/task1"
			URI uri = new URI(dstPath);
			try { 
				FileSystem hdfs = FileSystem.get(uri,conf);
				FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, conf, null); 
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}