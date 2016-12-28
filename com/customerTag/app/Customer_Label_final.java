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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sh.excel.AppLabel_Excel_Reader;

public class Customer_Label_final {
	private static class LabelMapper extends
			Mapper<NullWritable, Writable, Text, Text> {
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
				Mapper<NullWritable, Writable, Text, Text>.Context context)
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
		    				StringBuffer outputValue = new StringBuffer();
		    				outputValue.append(val[1]);
		    				outputValue.append(TAB);
		    				outputValue.append(score_final);
		    				context.write(new Text(outputKey.toString()),new Text(outputValue.toString()));
		        		}
		        	}
		        }
		    }catch(Exception e){};
		}
	}
		
	private static class LabelReducer extends
			Reducer<Text, Text, NullWritable, Writable> {
		private static HashMap<String,String> label_conf = new HashMap<String,String>();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String ini="/user/hadoop/app_label_conf/Label_conf.txt";
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
			HashMap<String,Double> label_score = new HashMap<String,Double>();
			double score = 0.0;
			String[] keys = key.toString().split("\\|");
			for(Text val:values){
				String[] vals = val.toString().split("\\|");
				if(label_score.get(vals[0].trim()) == null){
					label_score.put(vals[0].trim(), Double.parseDouble(vals[1]));
				}else{
					score = label_score.get(vals[0].trim()) + Double.parseDouble(vals[1]);
					label_score.put(vals[0].trim(), score);
				}
			}
			
			//Ð´orcÎÄ¼þ
			String[] result = new String[4];
			System.arraycopy(keys, 0, result, 0, keys.length);
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(Customer_Label_Row.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			
			String label = new String();
			String typeconf = new String();
			double score_final = 0.0;
			if((typeconf = label_conf.get(keys[1])) != null){
				if(typeconf.equals("cover")){
					Iterator ite = label_score.entrySet().iterator();
					while(ite.hasNext()){
						Map.Entry entry = (Map.Entry)ite.next();
						double tmp = (double)entry.getValue();
						if(tmp>=score_final){
							label = (String)entry.getKey();
							score_final = tmp;
						}
					}
					result[2] = label;
					result[3] = Double.toString(score_final);
					row = orcSerde.serialize(new Customer_Label_Row(result), inspector);
					context.write(NullWritable.get(), row);
				}else if(typeconf.equals("standby")){
					Iterator ite = label_score.entrySet().iterator();
					while(ite.hasNext()){
						Map.Entry entry = (Map.Entry)ite.next();
						result[2] = (String)entry.getKey();
						result[3] = Double.toString((double)entry.getValue());
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
		String month = args[0];
		String inputPath = "/user/hive/warehouse/phone.db/user_app_score_v6/reportmonth=" + month;
		String outputPath = "/user/hive/warehouse/phone.db/customer_label_v2/reportmonth=" + month;
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "customer_label_v2");
		job.addCacheFile(new URI("/user/hadoop/app_label_conf/label_v2.4_final.xlsx"));
		job.setJarByClass(Customer_Label_final.class);
		job.setNumReduceTasks(240);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		
		FileSystem fs = FileSystem.get(conf);
		FileInputFormat.addInputPaths(job, inputPath);
		if (fs.exists(new Path(outputPath))) {
		fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(LabelMapper.class);
		job.setReducerClass(LabelReducer.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
