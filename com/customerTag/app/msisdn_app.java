package com.customerTag.app;
/*根据phone_user表与host-appid字典(host_app表)进行join操作，得出msisdn-appid匹配结果
 * 该方法已弃用，使用HQL进行替代，效率更高*/
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class msisdn_app {
	
	private static final String TAB = "|";
	/**
	   * MapReduce作业1 的Mapper
	   * 
	   * **/
	private static class userMapper extends
			Mapper<NullWritable, Writable, Text, NullWritable> {
		private static final String SCHEMA = "struct<msisdn:string,tac:string,host:string,flow:float,cnt:float,time:float,prot_category:string,prot_type:string,rat:string,reportdate:string,hour:int>";
		private static HashMap<String,String> hm = new HashMap<String,String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
    		String ini="/user/hadoop/SH/host_app/host_app.txt";
			FSDataInputStream instr = fs.open(new Path(ini));
			BufferedReader buf = new BufferedReader(new InputStreamReader(instr));
			String dpi = null;
			while ((dpi = buf.readLine()) != null) {
				String[] rules = dpi.split(",");
				hm.put(rules[0], rules[1]);
			}
			if (buf != null) {
				buf.close();
			}
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
			    String msisdn = inspector.getStructFieldData(struct, inspector.getStructFieldRef("msisdn")).toString().trim();
		        String host = inspector.getStructFieldData(struct, inspector.getStructFieldRef("host")).toString().trim();
		        //读partition分区值;
                FileSplit filepieces = (FileSplit) context.getInputSplit();
    			//读取reportdate值
    			String filepath = filepieces.getPath().toString();
    			String reportdate = "";
    			Pattern p1 = Pattern.compile("reportdate=(.*?)\\/");
        		Matcher matcher1 = p1.matcher(filepath);
				if (matcher1.find()){
					reportdate = matcher1.group(1);
				}
				//读取hour值
				String hour = "";
    			Pattern p2 = Pattern.compile("hour=(.*?)\\/");
        		Matcher matcher2 = p2.matcher(filepath);
				if (matcher2.find()){
					hour = matcher2.group(1);
				}
				//进行应用匹配且写入context;
		        if (host != null && !"".equals(host) && msisdn != null && !"".equals(msisdn)){
		        	if(host.contains(":")){
		        		host = host.split(":",-1)[0];
		        	}
		        	String appid = "";
		        	if((appid = hm.get(host)) != null){
		        		StringBuffer outputKey = new StringBuffer();
    			        outputKey.append(msisdn);
    			        outputKey.append(TAB);
    			        outputKey.append(appid);
    			        outputKey.append(TAB);
    			        outputKey.append(reportdate);
    			        outputKey.append(TAB);
    			        outputKey.append(hour);
		        		context.write(new Text(outputKey.toString()),NullWritable.get());
		        	}
		        	
		        }
		    }catch(Exception e){};
		}
	}
	
	private static class userReducer extends
		Reducer<Text, NullWritable, NullWritable, Writable> {
			
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			//写orc file格式;
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(msisdn_appRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			row = orcSerde.serialize(new msisdn_appRow(key.toString().trim().split("\\|")), inspector);
			
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
		
		String startdate = args[0];
		String enddate = args[1];
		
		String job_baseInputPath = "/user/hive/warehouse/phone.db/phone_user/reportdate=";
		String job_inputPath = "";
		String job_outputPath = "/user/hadoop/SH/msisdn_app/reportmonth=" + startdate.substring(0, 6);
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "msisdn_app");
		job.setJarByClass(msisdn_app.class);
		job.setNumReduceTasks(60);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		
		//根据输入日期计算输入路径;
		FileSystem fs = FileSystem.get(conf);
		Date Start_Date = new SimpleDateFormat("yyyyMMdd").parse(startdate);//定义起始日期
		Date End_Date = new SimpleDateFormat("yyyyMMdd").parse(enddate);//定义结束日期
		Calendar dd = Calendar.getInstance(Locale.CHINA);//定义日期实例
		dd.setTime(Start_Date);//设置日期起始时间
		while(dd.getTime().before(End_Date)){//判断是否到结束日期
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String dates = sdf.format(dd.getTime());
			//检查输入文件夹是否存在；
			if (fs.exists(new Path(job_baseInputPath + dates + "/"))) {
				job_inputPath = job_inputPath + job_baseInputPath + dates + "/,";
			}
			dd.add(Calendar.DATE, 1);//进行当前日期月份加1
		}
		if (fs.exists(new Path(job_baseInputPath + enddate + "/"))) {
			job_inputPath = job_inputPath + job_baseInputPath + enddate + "/";
		}else{
			job_inputPath = job_inputPath.substring(0, job_inputPath.length()-1);
		}
		System.out.println(job_inputPath);
		FileInputFormat.addInputPaths(job, job_inputPath);
		if (fs.exists(new Path(job_outputPath))) {
		fs.delete(new Path(job_outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(job_outputPath));
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(userMapper.class);
		job.setReducerClass(userReducer.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
