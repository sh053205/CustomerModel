package com.customerTag.ML;

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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

import sh.excel.AppLabel_Excel_Reader;

public class V5_Vectorizing {
	private static class AppIdMapper extends
			Mapper<NullWritable, Writable, Text, NullWritable> {
		private static final String TAB = "|";
		private static final String SCHEMA = "struct<msisdn:string,appid:string,score:double,reportmonth:string>";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
		}
		
		@Override
		protected void map(
				NullWritable key, 
				Writable value,
				Mapper<NullWritable, Writable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			OrcStruct struct = (OrcStruct)value;
			TypeInfo typeInfo =
		            TypeInfoUtils.getTypeInfoFromTypeString(SCHEMA);
		    StructObjectInspector inspector = (StructObjectInspector)
		            OrcStruct.createObjectInspector(typeInfo);
		   try{
		        String appid = inspector.getStructFieldData(struct, inspector.getStructFieldRef("appid")).toString().trim();
		        if(appid!=null && !"".equals(appid)){
		        	context.write(new Text(appid),NullWritable.get());
		        }
		    }catch(Exception e){};
		}
	}
	
	private static class AppIdReducer extends
			Reducer<Text, NullWritable, Text, NullWritable > {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	private static class TransformMapper extends
			Mapper<NullWritable, Writable, Text, Text> {
		private static final String SCHEMA = "struct<msisdn:string,appid:string,score:double,reportmonth:string>";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
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
		        String score = inspector.getStructFieldData(struct, inspector.getStructFieldRef("score")).toString().trim();
		        if(msisdn!=null && !"".equals(msisdn) && appid!=null && !"".equals(appid) && score!=null && !"".equals(score)){
		        	StringBuffer outputValue = new StringBuffer();
		        	outputValue.append(appid).append("|").append(score);
		        	context.write(new Text(msisdn),new Text(outputValue.toString()));
		        }
		    }catch(Exception e){};
		}
	}
		
	private static class TransformReducer extends
			Reducer<Text, Text, Text, NullWritable > {
		private static List<Integer> AppList = new ArrayList<Integer>();
		//private static LinkedHashMap<String,Double> appscore = new LinkedHashMap<String,Double>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String thisMonth = DefaultStringifier.load(conf, "month", Text.class).toString();
			FileSystem fs = FileSystem.get(conf);
			//检查输入文件夹是否存在;
			String ini="/user/hadoop/SH/appid_temp/reportmonth=" + thisMonth;
			FileStatus[] status = fs.listStatus(new Path(ini));
			if(status.length>0){
				for(FileStatus file:status){
					FSDataInputStream in = fs.open(file.getPath());
					BufferedReader bf = new BufferedReader(new InputStreamReader(in));
					String str = null;
					while ((str = bf.readLine()) != null) {
						//appscore.put(str.trim(), 0.0);
						AppList.add(Integer.parseInt(str.trim()));
					}
					if (bf != null) {
						bf.close();
					}
				}
			}
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String,Double> hm = new HashMap<String,Double>();
			//把用户本月记录存入hashmap；
			for(Text line:values){
				String[] val = line.toString().split("\\|");
				hm.put(val[0].trim(), Double.parseDouble(val[1].trim()));
			}
			StringBuffer sb = new StringBuffer();
			sb.append(key.toString().trim()).append("\t").append(0).append("\t").append(9).append("\t");
			java.util.Collections.sort(AppList);
			for(int appid:AppList){
				if(hm.get(Integer.toString(appid)) != null){
	        		sb.append(hm.get(Integer.toString(appid))).append("\t");
	        	}else{
	        		sb.append(0.0).append("\t");
	        	}
			}
			/*Iterator ite = appscore.entrySet().iterator();
			while(ite.hasNext()){
				Map.Entry entry = (Map.Entry)ite.next();
				String appid = (String)entry.getKey();
				double score = (double)entry.getValue();
				if(hm.get(appid) != null){
	        		sb.append(hm.get(appid)).append("\t");
	        	}else{
	        		sb.append(score).append("\t");
	        	}
			}*/
			sb.deleteCharAt(sb.length()-1);
			context.write(new Text(sb.toString()), NullWritable.get());
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
		String inputPath = "/user/hive/warehouse/phone.db/user_app_score_v5/reportmonth=" + month;
		String temp_path = "/user/hadoop/SH/appid_temp/reportmonth=" + month;
		String outputPath = "/user/hadoop/v5_vectorize/reportmonth=" + month;
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		DefaultStringifier.store(conf, new Text(month), "month");
		FileSystem fs = FileSystem.get(conf);
		Path appid_path = new Path("/user/hadoop/appid/reportmonth="+month+"/appidlist.txt");
		if (fs.exists(appid_path)) {
			fs.delete(appid_path, true);
		}
		Job job1 = Job.getInstance(conf, "v5_vectorizing");
		job1.setJarByClass(V5_Vectorizing.class);
		job1.setNumReduceTasks(240);
		job1.setInputFormatClass(OrcNewInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPaths(job1, inputPath);
		if (fs.exists(new Path(temp_path))) {
		fs.delete(new Path(temp_path), true);
		}
		FileOutputFormat.setOutputPath(job1, new Path(temp_path));
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		job1.setMapperClass(AppIdMapper.class);
		job1.setReducerClass(AppIdReducer.class);
		job1.waitForCompletion(true);
		
		//将中间结果appid排序后输出;
		List<Integer> AppList = new ArrayList<Integer>();
		FileStatus[] status = fs.listStatus(new Path(temp_path));
		if(status.length>0){
			for(FileStatus file:status){
				FSDataInputStream in = fs.open(file.getPath());
				BufferedReader bf = new BufferedReader(new InputStreamReader(in));
				String str = null;
				while ((str = bf.readLine()) != null) {
					//appscore.put(str.trim(), 0.0);
					AppList.add(Integer.parseInt(str.trim()));
				}
				if (bf != null) {
					bf.close();
				}
			}
		}
		//将appid排序后写入hdfs;
		java.util.Collections.sort(AppList);
		FSDataOutputStream fo = fs.create(appid_path);
		for(int appid:AppList){
			byte[] readBuf = Integer.toString(appid).getBytes("UTF-8");
			fo.write(readBuf, 0, readBuf.length);
		}
		fo.close();
		
		Job job2 = Job.getInstance(conf, "v5_vectorize");
		job2.setJarByClass(V5_Vectorizing.class);
		job2.setNumReduceTasks(240);
		job2.setInputFormatClass(OrcNewInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job2, inputPath);
		if (fs.exists(new Path(outputPath))) {
		fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		job2.setMapperClass(TransformMapper.class);
		job2.setReducerClass(TransformReducer.class);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
