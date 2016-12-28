package com.customerTag.app;
/*该类根据phone.user_app_tmp表进行客户app使用情况统计，包括的统计数据为：
 * 1.该月每天平均使用小时数；
 * 2.该月使用天数
 * 3.该月使用小时数；
 * 4.该月使用次数*/
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

public class user_app {
	
	private static class AppMapper extends
			Mapper<NullWritable, Writable, Text, Text> {
		private static final String TAB = "|";
		private static List<String[]> DictList = new ArrayList<String[]>();
		private static final String SCHEMA = "struct<msisdn:string,appid:string,cnt:float,reportdate:string,hour:int>";
		
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
                String cnt = inspector.getStructFieldData(struct, inspector.getStructFieldRef("cnt")).toString().trim();
                String reportdate = inspector.getStructFieldData(struct, inspector.getStructFieldRef("reportdate")).toString().trim();
                String hour = inspector.getStructFieldData(struct, inspector.getStructFieldRef("hour")).toString().trim();
                               				
                if(cnt == null || "".equals(cnt) ){
                	cnt = "0.0";
                }
    			StringBuffer outputKey = new StringBuffer();
    			outputKey.append(msisdn);
    			outputKey.append(TAB);
				outputKey.append(appid);
				outputKey.append(TAB);
				outputKey.append(reportdate.substring(0, 6));
				StringBuffer outputValue = new StringBuffer();
				outputValue.append(reportdate);
				outputValue.append(TAB);
				outputValue.append(hour);
				outputValue.append(TAB);
				outputValue.append(cnt);
				context.write(new Text(outputKey.toString()),new Text(outputValue.toString()));

            }catch(Exception e){};
		}
	}

	private static class AppReducer extends
			Reducer<Text, Text, NullWritable, Writable > {
				
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			double hoursPerDay = 0.00;
			int hoursThisMonth = 0;
			int daysThisMonth = 0;
			float timesThisMonth = 0.0f;
			List<String> hourlist = new LinkedList<String>();
			List<String> daylist = new LinkedList<String>();
			
			//计算该月总共天数;
			String[] keys = key.toString().trim().split("\\|");
			Calendar aCalendar = Calendar.getInstance(Locale.CHINA);
			aCalendar.set(Integer.parseInt(keys[2].substring(0, 4)), Integer.parseInt(keys[2].substring(4).trim()), 01);
			int day=aCalendar.getActualMaximum(Calendar.DATE);
			
			//统计数据计算
			for(Text val:values){
				String[] vals = val.toString().trim().split("\\|");
				//以小时为单位统计一次
				String reporthour = vals[0]+vals[1];
				if(!hourlist.contains(reporthour)){
					hourlist.add(reporthour);
				}
				//以每天为单位统计一次
				String reportdate = vals[0];
				if(!daylist.contains(reportdate)){
					daylist.add(reportdate);
				}
				//计算总次数
				timesThisMonth = timesThisMonth + Float.parseFloat(vals[2]);
			}
			hoursThisMonth = hourlist.size();
			hoursPerDay = (float)hoursThisMonth/day;
			daysThisMonth = daylist.size();
			
			//写orc file格式;
			String[] result = new String[7];
			System.arraycopy(keys, 0, result, 0, keys.length);
			result[3] = Double.toString(hoursPerDay);
			result[4] = Integer.toString(daysThisMonth);
			result[5] = Integer.toString(hoursThisMonth);
			result[6] = Float.toString(timesThisMonth);
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(AppPrefsRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			row = orcSerde.serialize(new AppPrefsRow(result), inspector);
			
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
		String month = args[0];
		String inputPath = "/user/hadoop/user_app_tmp/reportmonth=" + month;
		String outputPath = "/user/hadoop/user_app/reportmonth=" + month;
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		
		Job job = Job.getInstance(conf, "user_app");
		job.setJarByClass(user_app.class);
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
		job.setMapperClass(AppMapper.class);
		job.setReducerClass(AppReducer.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
