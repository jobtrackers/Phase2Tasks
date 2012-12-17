package org.myorg;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import org.format.XmlInputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;


public class Task4 {
	public static class Task4Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable>output, Reporter reporter) throws IOException {
			String xmlString;
			SAXBuilder builder;
			Reader in;
			Document doc;
			Element root;
			String Recency_date;
			String parsed_String;
			String Recency;
		
			xmlString = value.toString();
			builder = new SAXBuilder();
			in = new StringReader(xmlString);
			
			try {
				

				doc = builder.build(in);
				root = doc.getRootElement();
				parsed_String =root.getChild("revision").getText().trim() ;
				Recency_date =root.getChild("revision").getChild("timestamp").getText().trim();
				Recency = Recency_date.split("T")[0];
				output.collect(new Text(Recency), new IntWritable(1));
					
			
			} catch (JDOMException ex) {
				Logger.getLogger(Task4Map.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IOException ex) {
				Logger.getLogger(Task4Map.class.getName()).log(Level.SEVERE, null, ex);
			}


		}

		

	}
	public static class Task4Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum+=values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task4.class);
		conf.setJobName("Task4");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		conf.setJarByClass(Task4Map.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Task4Map.class);
		conf.setCombinerClass(Task4Reduce.class);
		conf.setReducerClass(Task4Reduce.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		
	}

}

