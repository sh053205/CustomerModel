package com.customerTag.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sh.excel.AppLabel_Excel_Reader;

public class Customer_Label_2job {
	private static class LabelMapper extends
			Mapper<NullWritable, Writable, Text, DoubleWritable> {
		private static final String TAB = "|";
		private static HashMap<String,List<String[]>> applabels = new HashMap<String,List<String[]>>();
		private static final String SCHEMA = "struct<msisdn:string,appid:string,score:double,reportmonth:string>";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			URI[] localPath = context.getCacheFiles();
			FSDataInputStream in = fs.open(new Path(localPath[0]));
	        applabels = AppLabel_Excel_Reader.XlsxReader(in);
		}
		
		@Override
		protected void map(
				NullWritable key, 
				Writable value,
				Mapper<NullWritable, Writable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
			OrcStruct struct = (OrcStruct)value;
			TypeInfo typeInfo =
		            TypeInfoUtils.getTypeInfoFromTypeString(SCHEMA);
		    
		    StructObjectInspector inspector = (StructObjectInspector)
		            OrcStruct.createObjectInspector(typeInfo);
		    
		   try{
		    	String msisdn = inspector.getStructFieldData(struct, inspector.getStructFieldRef("msisdn")).toString().trim();
		        String appid = inspector.getStructFieldData(struct, inspector.getStructFieldRef("appid")).toString().trim();
		        double score = Double.parseDouble(inspector.getStructFieldData(struct, inspector.getStructFieldRef("score")).toString().trim());
		        if(msisdn!=null && !"".equals(msisdn)){
		        	List<String[]> labels = new ArrayList<String[]>();
		        	if((labels = applabels.get(appid)) != null){
		        		for(String[] val : labels){
		        			double score_final = score*Double.parseDouble(val[2]);
		        			StringBuffer outputKey = new StringBuffer();
		    				outputKey.append(msisdn);
		    				outputKey.append(TAB);
		    				outputKey.append(val[0]);
		    				outputKey.append(TAB);
		    				outputKey.append(val[1]);
		    				context.write(new Text(outputKey.toString()),new DoubleWritable(score_final));
		        		}
		        	}
		        }
		    }catch(Exception e){};
		}
	}
		
	private static class LabelReducer extends
			Reducer<Text, DoubleWritable, Text, NullWritable > {
				
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double score = 0.0;
			StringBuffer outputKey = new StringBuffer();
			outputKey.append(key.toString()).append("|");
			for(DoubleWritable val : values){
				score = score + val.get();
			}
			outputKey.append(score);
			context.write(new Text(outputKey.toString()), NullWritable.get());
		}
	}
	
	private static class FilterMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private static final String TAB = "|";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
		}
		
		@Override
		protected void map(
				LongWritable key, 
				Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		    
			String[] val = value.toString().split("\\|");
			StringBuffer outputKey = new StringBuffer();
			outputKey.append(val[0]);
			outputKey.append(TAB);
			outputKey.append(val[1]);
			StringBuffer outputValue = new StringBuffer();
			outputValue.append(val[2]);
			outputValue.append(TAB);
			outputValue.append(val[3]);
			context.write(new Text(outputKey.toString()),new Text(outputValue.toString()));
			
		}
	}
		
	private static class FilterReducer extends
			Reducer<Text, Text, NullWritable, Writable > {

		private static HashMap<String,String> label_conf = new HashMap<String,String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String ini="/user/hadoop/SH/Label_conf.txt";
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(ini));
			BufferedReader bf = new BufferedReader(new InputStreamReader(in));
			String str = null;
			while ((str = bf.readLine()) != null) {
				String[] rules = str.split(",");
				label_conf.put(rules[0].trim(), rules[1].trim());
			}
			if (bf != null) {
				bf.close();
			}
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double score = 0.0;
			String label = "";
			String[] keys = key.toString().split("\\|");
			String typeconf = new String();
			//Ð´orcÎÄ¼þ
			String[] result = new String[4];
			System.arraycopy(keys, 0, result, 0, keys.length);
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(Customer_Label_Row.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			
			if((typeconf = label_conf.get(keys[1].trim())) != null){
				if(typeconf.equals("cover")){
					for(Text val:values){
						String[] vals = val.toString().split("\\|");
						double score_cmp = Double.parseDouble(vals[1]);
						if(score_cmp>=score){
							score = score_cmp;
							label = vals[0];
						}
					}
					result[2] = label;
					result[3] = Double.toString(score);
					row = orcSerde.serialize(new Customer_Label_Row(result), inspector);
					context.write(NullWritable.get(), row);
				}else if(typeconf.equals("standby")){
					for(Text val:values){
						String[] vals = val.toString().split("\\|");
						result[2] = vals[0];
						result[3] = vals[1];
						row = orcSerde.serialize(new Customer_Label_Row(result), inspector);
						context.write(NullWritable.get(), row);
					}
				}
			}
		}
	}
	
		
		/**
		* @param args
		* @throws URISyntaxException
		* @throws IOException
		* @throws ClassNotFoundException
		* @throws InterruptedException
		* @throws ParseException 
		*/
	public static void main(String[] args) throws IOException,
			URISyntaxException, InterruptedException, ClassNotFoundException, ParseException {
		String uri = args[0];
		String inputPath = "/user/hive/warehouse/phone.db/user_app_score_v5/reportmonth=201602";
		String temp_path = "/user/hadoop/SH/temp/";
		String outputPath = "/user/hadoop/customer_label/reportmonth=201602";
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job1 = Job.getInstance(conf, "customer_label");
		job1.addCacheFile(new URI(uri));
		job1.setJarByClass(Customer_Label_2job.class);
		job1.setNumReduceTasks(240);
		job1.setInputFormatClass(OrcNewInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileSystem fs = FileSystem.get(conf);
		FileInputFormat.addInputPaths(job1, inputPath);
		if (fs.exists(new Path(temp_path))) {
		fs.delete(new Path(temp_path), true);
		}
		FileOutputFormat.setOutputPath(job1, new Path(temp_path));
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		job1.setMapperClass(LabelMapper.class);
		job1.setReducerClass(LabelReducer.class);
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "customer_label");
		job2.setJarByClass(Customer_Label_2job.class);
		job2.setNumReduceTasks(240);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(OrcNewOutputFormat.class);
		FileInputFormat.addInputPaths(job2, temp_path);
		if (fs.exists(new Path(outputPath))) {
		fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Writable.class);
		job2.setMapperClass(FilterMapper.class);
		job2.setReducerClass(FilterReducer.class);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
