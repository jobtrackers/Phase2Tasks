package org.myorg;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
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
import org.format.XmlInputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;




public class Task3_numbers {
	public static class Task3Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable>output, Reporter reporter) throws IOException {
			String xmlString;
			SAXBuilder builder;
			Reader in;
			Document doc;
			Element root;
			List<Element> elementList= new ArrayList<Element>();

			xmlString = value.toString();
			builder = new SAXBuilder();
			in = new StringReader(xmlString);
			try {

				doc = builder.build(in);
				root = doc.getRootElement();
				Element revision=root.getChild("revision");
				elementList =revision.getChildren("crosslanguage");
				Iterator<Element> iterator = elementList.iterator();
				while (iterator.hasNext()) {
					Element child = (Element) iterator.next();
					String crossLangKey_temp=child.getText().trim();
					String crossLangKey=crossLangKey_temp.toLowerCase(Locale.ENGLISH);
					output.collect(new Text(crossLangKey),one);
					
				}
				
			} catch (JDOMException ex) {
				Logger.getLogger(Task3Map.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IOException ex) {
				Logger.getLogger(Task3Map.class.getName()).log(Level.SEVERE, null, ex);
			}

		}

	}
	public static class Task3Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		String input=args[0];
		String output_multi=args[1];
		JobConf conf = new JobConf(Task3_numbers.class);
		conf.setJobName("task3_numbers");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Task3Map.class);
		conf.setCombinerClass(Task3Reduce.class);
		conf.setReducerClass(Task3Reduce.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		String outdir=output_multi+"/task3_1";
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(outdir));

		RunningJob runjob =JobClient.runJob(conf);
		if(runjob.isComplete()){
			try { 
				
				Thread.sleep(40000);
				Path output=FileOutputFormat.getOutputPath(conf);
				
				String dstPath = output_multi+"/task3_merged/";//"s3n://phase2mergedout/task1"
				URI uri = new URI(dstPath);
				FileSystem hdfs = FileSystem.get(uri,conf);
				FileUtil.copyMerge(hdfs, output, hdfs, new Path(dstPath), false, conf, null); 
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}
