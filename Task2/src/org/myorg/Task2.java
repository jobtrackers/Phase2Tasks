package org.myorg;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Task2 {


	public static class Task2_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		//private final static IntWritable one = new IntWritable(1);
		//private Text template = new Text("template");
		//private Text no_template = new Text("no_template");

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			Pattern p1=Pattern.compile("(.*?),\"?\\{\\{Infobox[\\s+]([\\w\\s]+)");
			Matcher m; 
			String line = value.toString();
			String modkey,modvalue;
			m = p1.matcher(line);
			if(m.find()){
				modkey=m.group(2).trim();
				modvalue=m.group(1).trim()+",";
				output.collect(new Text(modkey), new Text(modvalue));
			}
			
		}
	}

	public static class Task2_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String str="";
			while (values.hasNext()) {
			str+=values.next();	
				
			}
			//str=str.substring(0, str.length()-1);
			output.collect(key, new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task2.class);
		conf.setJobName("task2");
		conf.setJarByClass(Task2_Map.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Task2_Map.class);
		conf.setCombinerClass(Task2_Reduce.class);
		conf.setReducerClass(Task2_Reduce.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}