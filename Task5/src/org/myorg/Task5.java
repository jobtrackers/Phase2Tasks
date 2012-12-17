package org.myorg;


import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.format.XmlInputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;





public class Task5 {
	public static class Task5Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private final static Text recent_key = new Text        ("Recent                "); 
		private final static Text twoMonths_key = new Text     ("Two months and old    ");
		private final static Text fourMonths_key = new Text    ("Four months and old   ");
		private final static Text sixMonths_key = new Text     ("Six months and old    ");
		private final static Text nineMonths_key = new Text    ("Nine months and old   ");
		private final static Text oneYear_key = new Text       ("One year and old      ");
		private final static Text oneYearandHalf_key = new Text("One.Six years and old ");
		private final static Text twoYears_key = new Text      ("Two years and old     ");
		private final static Text threeYears_key = new Text    ("Three years and old   ");
		private final static Text fourYears_key = new Text     ("Four years and old    ");
		private final static Text fiveYears_key = new Text     ("Five years and old    ");
		private final static Text sixYears_key = new Text      ("Six years and old     ");
		private final static Text sevenYears_key = new Text    ("Seven years and old   ");
		private final static Text eightYears_key = new Text    ("Eight years and old   ");
		private final static Text tenYears_key = new Text      ("Ten years and old     ");
		private final static Text unidentified_key = new Text  ("Date Unknown          ");
		public void map(LongWritable key, Text value, OutputCollector<Text,Text>output, Reporter reporter) throws IOException {
			String xmlString;
			SAXBuilder builder;
			Reader in;
			Document doc;
			Element root;
			String parsedTime_temp,parsed_Time;
			String parsedTitle;


			Element revision;
			xmlString = value.toString();
			builder = new SAXBuilder();
			in = new StringReader(xmlString);
			try {

				doc = builder.build(in);
				root = doc.getRootElement();
				parsedTitle =root.getChild("title").getText().trim() ;
				revision=root.getChild("revision");
				parsedTime_temp =revision.getChild("timestamp").getText().trim();
				parsed_Time=parsedTime_temp.split("T")[0];

				Text keyTobePassed=getComparisionResult(parsed_Time);
				output.collect(keyTobePassed,new Text(parsedTitle));


			} catch (JDOMException ex) {
				Logger.getLogger(Task5Map.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IOException ex) {
				Logger.getLogger(Task5Map.class.getName()).log(Level.SEVERE, null, ex);
			} 

		}

		public String getPastDate(int days,SimpleDateFormat dateFormat) {
			Calendar addDate=Calendar.getInstance();
			addDate.add(Calendar.DATE,days);
			String returnDate=dateFormat.format(addDate.getTime());
			//System.out.println("Date returned:"+returnDate);
			return (returnDate);

		}
		public Text getComparisionResult(String parsedTime){
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String currDate = dateFormat.format(new Date());
			String twoMonths= getPastDate(-60,dateFormat);
			String fourMonths= getPastDate(-120,dateFormat);
			String sixMonths= getPastDate(-180,dateFormat);
			String nineMonths= getPastDate(-270,dateFormat);
			String oneYear= getPastDate(-365,dateFormat);
			String oneYearandhalf= getPastDate(-545,dateFormat);
			String twoYears= getPastDate(-730,dateFormat);
			String threeYears= getPastDate(-1095,dateFormat);
			String fourYears= getPastDate(-1460,dateFormat);
			String fiveYears= getPastDate(-1826,dateFormat);
			String sixYears= getPastDate(-2190,dateFormat);
			String sevenYears= getPastDate(-2555,dateFormat);
			String eightYears= getPastDate(-2920,dateFormat);
			String tenYears= getPastDate(-3650,dateFormat);

			try{
				if((dateFormat.parse(parsedTime).before(dateFormat.parse(fourYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(fiveYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(fourYears)))
					return fourYears_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(fiveYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(sixYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(fiveYears)))
					return fiveYears_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(sixYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(sevenYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(sixYears)))
					return sixYears_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(sevenYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(eightYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(sevenYears)))
					return sevenYears_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(eightYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(tenYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(eightYears)))
					return eightYears_key;
				
				
				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(sixMonths)) && dateFormat.parse(parsedTime).after(dateFormat.parse(nineMonths)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(sixMonths)))
					return sixMonths_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(nineMonths)) && dateFormat.parse(parsedTime).after(dateFormat.parse(oneYear)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(nineMonths)))
					return nineMonths_key;


				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(oneYear)) && dateFormat.parse(parsedTime).after(dateFormat.parse(oneYearandhalf)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(oneYear)))
					return oneYear_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(oneYearandhalf)) && dateFormat.parse(parsedTime).after(dateFormat.parse(twoYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(oneYearandhalf)))
					return oneYearandHalf_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(twoYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(threeYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(twoYears)))
					return twoYears_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(threeYears)) && dateFormat.parse(parsedTime).after(dateFormat.parse(fourYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(threeYears)))
					return threeYears_key;
				
				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(currDate))&& dateFormat.parse(parsedTime).after(dateFormat.parse(twoMonths)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(currDate)))
					return recent_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(twoMonths)) && dateFormat.parse(parsedTime).after(dateFormat.parse(fourMonths)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(twoMonths)))
					return twoMonths_key;

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(fourMonths)) && dateFormat.parse(parsedTime).after(dateFormat.parse(sixMonths)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(fourMonths)))
					return fourMonths_key;
				

				else if((dateFormat.parse(parsedTime).before(dateFormat.parse(tenYears)))||dateFormat.parse(parsedTime).equals(dateFormat.parseObject(tenYears)))
					return tenYears_key;
				
				else{
					//System.out.println(parsedTime);
					return unidentified_key;
				}
			}
			catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return unidentified_key;

		}

	}
	public static class Task5Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			StringBuilder value_articles=new StringBuilder("");
			while (values.hasNext()) {
				value_articles.append(values.next().toString()+",");
			}
			output.collect(key, new Text(value_articles.toString()));
		}
	}


	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task5.class);
		conf.setJobName("Task5");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMaxMapTaskFailuresPercent(50);
		conf.setMaxReduceTaskFailuresPercent(50);
		conf.setMapperClass(Task5Map.class);
		conf.setCombinerClass(Task5Reduce.class);
		conf.setReducerClass(Task5Reduce.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));//same input like task 3
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}

}
