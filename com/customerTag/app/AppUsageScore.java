package com.customerTag.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * 根据用户半年内app持续使用情况打分，使用连续性越好，分数越高
 * 改分数仅考虑客户连续使用情况，对月内使用次数不作考虑，该方法已弃用
 * */
public class AppUsageScore {

	private static final HashMap<String, Double> Scores = new HashMap<String, Double>();
	
	private static class ExtractorMapper extends
	Mapper<NullWritable, Writable, Text, Text> {
	private static final String TAB = "|";
	private static final String SCHEMA = "struct<msisdn:string,appid:string,month:string,timesperday:string,timesthismonth:string,reportmonth:string>";
	
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
		        String month = inspector.getStructFieldData(struct, inspector.getStructFieldRef("month")).toString().trim();
				//进行应用匹配且写入context;
		        if (msisdn != null && !"".equals(msisdn) && appid != null && !"".equals(appid)){
		        	
					StringBuffer outputKey = new StringBuffer();
	    			outputKey.append(msisdn);
	    			outputKey.append(TAB);
					outputKey.append(appid);
					StringBuffer outputValue = new StringBuffer();
					outputValue.append(month);
					context.write(new Text(outputKey.toString()),new Text(outputValue.toString()));
					
		        }
		    }catch(Exception e){};
		}
	}
	
	private static class ExtractorReducer extends
		Reducer<Text, Text, NullWritable, Writable > {
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String thisMonth = DefaultStringifier.load(conf, "month", Text.class).toString();
			Date currentMonth = new Date();
			try {
				currentMonth = new SimpleDateFormat("yyyyMM").parse(thisMonth);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}//定义起始日期
			Calendar dd = Calendar.getInstance(Locale.CHINA);//定义日期实例
			dd.setTime(currentMonth);//设置日期起始时间
			dd.add(Calendar.MONTH, -5);
			double score = 0.1;	//评分
			while(dd.getTime().before(currentMonth)){//判断是否到结束日期
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
				String iterator_month = sdf.format(dd.getTime());
				Scores.put(iterator_month, score);
				dd.add(Calendar.MONTH, 1);//进行当前日期月份加1
				score = score + 0.1f;
			}
			Scores.put(thisMonth, score);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			double scores = 0.00;
			List<String> monthlist = new LinkedList<String>();
			String[] keys = key.toString().split("\\|"); 
			//统计app使用月份
			for(Text val:values){
				String month = val.toString().trim();
				if(!monthlist.contains(month)){
					monthlist.add(month);
				}
			}
			Collections.sort(monthlist);
			//循环统计使用月份，且打分;
			for(int i = 0;i < monthlist.size();i++){
				for(int j = 0;j <= i;j++){
					scores = scores + Scores.get(monthlist.get(j));
				}
			}
			
			scores = scores/5.6;
			DecimalFormat df = new DecimalFormat("0.00");
			
			//写orc file格式;
			String[] result = new String[3];
			System.arraycopy(keys, 0, result, 0, keys.length);
			result[2] = df.format(scores);
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(AppUsageScoreRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			row = orcSerde.serialize(new AppUsageScoreRow(result), inspector);
			
			context.write(NullWritable.get(), row);
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
	
		String inputPath = "";
		String baseInputPath = "";
		if(args[0].endsWith("/")){
			baseInputPath = args[0]+"reportmonth=";
		}else{
			baseInputPath = args[0]+"/reportmonth=";
		}
		String month = args[1];
		String outputPath = "";
		if(args[2].endsWith("/")){
			outputPath = args[2];
		}else{
			outputPath = args[2]+"/";
		}
		outputPath = outputPath + "reportmonth=" + month;
		
		Configuration conf = new Configuration();
		DefaultStringifier.store(conf,new Text(month),"month");
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "App Usage Frequency");
		job.setJarByClass(AppUsageScore.class);
		job.setNumReduceTasks(120);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		//根据输入日期计算输入路径;
		Date currentMonth = new SimpleDateFormat("yyyyMM").parse(month);//定义起始日期
		Calendar dd = Calendar.getInstance(Locale.CHINA);//定义日期实例
		dd.setTime(currentMonth);//设置日期起始时间
		dd.add(Calendar.MONTH, -5);
		while(dd.getTime().before(currentMonth)){//判断是否到结束日期
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
			String iterator_month = sdf.format(dd.getTime());
			//检查输入文件夹是否存在；
			if (fs.exists(new Path(baseInputPath + iterator_month + "/"))) {
				inputPath = inputPath + baseInputPath + iterator_month + "/,";
			}
			dd.add(Calendar.MONTH, 1);//进行当前日期月份加1
		}
		if (fs.exists(new Path(baseInputPath + month + "/"))) {
			inputPath = inputPath + baseInputPath + month + "/";
		}else{
			inputPath = inputPath.substring(0, inputPath.length()-1);
		}
		FileInputFormat.addInputPaths(job, inputPath);
		
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
			}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(ExtractorMapper.class);
		job.setReducerClass(ExtractorReducer.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
