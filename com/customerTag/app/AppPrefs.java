package com.customerTag.app;
/*客户app使用情况统计，包括的统计数据为：
 *	 1.该月每天平均使用小时数；
 *	 2.该月使用天数
 * 该方法为最初测试阶段读取种子用户（phone.phone_seed）方法，
 * 实际产品中已弃用*/
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class AppPrefs {
	private static class ExtractorMapper extends
			Mapper<NullWritable, Writable, Text, Text> {
		private static final String TAB = "|";
		private static List<String[]> DPIList = new ArrayList<String[]>();
		private static final String SCHEMA = "struct<msisdn:string,host:string,flow:float,cnt:float,time:float,reportdate:string,hour:int>";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			//读取hdfs上的appRules库
			Configuration conf = context.getConfiguration();
			
			String ini="/user/hive/warehouse/label.db/apprules/apprules.txt";
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(ini));
			BufferedReader bf = new BufferedReader(new InputStreamReader(in));
			String str = null;
			while ((str = bf.readLine()) != null) {
				String[] rules = str.split(",");
				DPIList.add(rules);
			}
			if (bf != null) {
				bf.close();
			}
		}

		@Override
		protected void map(
				NullWritable key, 
				Writable value,
				Mapper<NullWritable, Writable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int flag = 0;
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
                if (msisdn != null && !"".equals(msisdn) && host != null && !"".equals(host)){
                	if(host.contains(":")){
                		host = host.split(":",-1)[0];
                	}
                	
                	int labelPrefs = 0;//优选label为0的标签;
                	String appid = new String();
                	int len1 = 0,len2 = 0;
    				for(String[] dpi:DPIList){
    					String host1 = dpi[4];
    					String host2 = dpi[5];
                		int label = Integer.parseInt(dpi[6].trim());
                		if(label == 0){
                			if(host.endsWith(host1) && host.contains(host2)){
    	        	    		flag = 1;
    	        	    		labelPrefs = 1;
    	        	    		//避免匹配到重复项
    	        	    		if(host1.length()>len1 || host2.length()>len2){
    	        	    			len1 = host1.length();
    		        	    		len2 = host2.length();
    		        	    		appid = dpi[0];
    	        	    		}
    	        	    	}
                		}else if(label == 1 && labelPrefs == 0){
                			if(host.endsWith(host1) || host.equals(host2)){
                				flag = 1;
                				//避免匹配到重复项
                				if(host1.length()>len1){
    	        	    			len1 = host1.length();
    		        	    		appid = dpi[0];
    	        	    		}
                			}
                		}
                	}
    				if(flag == 1){
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
    					context.write(new Text(outputKey.toString()),new Text(outputValue.toString()));
    				}
                }
            }catch(Exception e){};
		}
	}

	private static class ExtractorReducer extends
			Reducer<Text, Text, NullWritable, Writable > {
		
		private MultipleOutputs<NullWritable,Writable> out;  
	    //创建MultipleOutputs对象  
	    protected void setup(Context context) throws IOException,InterruptedException {  
	        out = new MultipleOutputs<NullWritable, Writable>(context);  
	     }
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			double timesPerDay = 0.00;
			int timesThisMonth = 0;
			List<String> hourlist = new LinkedList<String>();
			List<String> daylist = new LinkedList<String>();
			
			//计算该月总共天数;
			String[] keys = key.toString().trim().split("\\|");
			Calendar aCalendar = Calendar.getInstance(Locale.CHINA);
			aCalendar.set(Integer.parseInt(keys[2].substring(0, 4)), Integer.parseInt(keys[2].substring(4).trim()), 01);
			int day=aCalendar.getActualMaximum(Calendar.DATE);
			
			//统计使用次数
			for(Text val:values){
				//以小时为单位统计一次
				String reporthour = val.toString().trim();
				if(!hourlist.contains(reporthour)){
					hourlist.add(reporthour);
				}
				//以每天为单位统计一次
				String reportdate = reporthour.substring(0, 8);
				if(!daylist.contains(reportdate)){
					daylist.add(reportdate);
				}
			}
			timesPerDay = (float)hourlist.size()/day;
			timesThisMonth = daylist.size();
			
			//写orc file格式;
			String[] result = new String[5];
			System.arraycopy(keys, 0, result, 0, keys.length);
			result[3] = Double.toString(timesPerDay);
			result[4] = Integer.toString(timesThisMonth);
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(AppPrefsRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			row = orcSerde.serialize(new AppPrefsRow(result), inspector);

			out.write("AppPrefs",NullWritable.get(), row, "reportmonth="+keys[2]+"/"+keys[2]);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			   out.close();
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
			baseInputPath = args[0]+"reportdate=";
		}else{
			baseInputPath = args[0]+"/reportdate=";
		}
		String startdate = args[1];
		String enddate = args[2];
		String outputPath = "";
		if(args[3].endsWith("/")){
			outputPath = args[3];
		}else{
			outputPath = args[3]+"/";
		}
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "App Preferences");
		job.setJarByClass(AppPrefs.class);
		//job.setNumReduceTasks(40);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		//根据输入日期计算输入路径;
		Date Start_Date = new SimpleDateFormat("yyyyMMdd").parse(startdate);//定义起始日期
		Date End_Date = new SimpleDateFormat("yyyyMMdd").parse(enddate);//定义结束日期
		Calendar dd = Calendar.getInstance(Locale.CHINA);//定义日期实例
		dd.setTime(Start_Date);//设置日期起始时间
		while(dd.getTime().before(End_Date)){//判断是否到结束日期
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String dates = sdf.format(dd.getTime());
			//检查输入文件夹是否存在；
			
			if (fs.exists(new Path(baseInputPath + dates + "/"))) {
				inputPath = inputPath + baseInputPath + dates + "/,";
			}
			dd.add(Calendar.DATE, 1);//进行当前日期月份加1
		}
		if (fs.exists(new Path(baseInputPath + enddate + "/"))) {
			inputPath = inputPath + baseInputPath + enddate + "/";
		}else{
			inputPath = inputPath.substring(0, inputPath.length()-1);
		}
		FileInputFormat.addInputPaths(job, inputPath);

		MultipleOutputs.addNamedOutput(job,"AppPrefs",OrcNewOutputFormat.class,NullWritable.class,Writable.class);
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
