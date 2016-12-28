package com.customerTag.app;
/*在group by host的结果基础上，进行host匹配appid工作，依据apprules
 * 其中hostgroupby工作由HQL进行，效率更高*/
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

public class host_app {
	
	private static final String TAB = "|";
	/**
	   * MapReduce作业1 的Mapper
	   * 
	   * **/
	/*private static class GroupbyMapper extends
			Mapper<NullWritable, Writable, Text, NullWritable> {
		private static final String SCHEMA = "struct<msisdn:string,tac:string,host:string,flow:float,cnt:float,time:float,prot_category:string,prot_type:string,rat:string,reportdate:string,hour:int>";
		
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
		        String host = inspector.getStructFieldData(struct, inspector.getStructFieldRef("host")).toString().trim();
		        
				//进行应用匹配且写入context;
		        if (host != null && !"".equals(host)){
		        	if(host.contains(":")){
		        		host = host.split(":",-1)[0];
		        	}
		        	context.write(new Text(host),NullWritable.get());
		        }
		    }catch(Exception e){};
		}
	}
	
	private static class GroupbyReducer extends
		Reducer<Text, NullWritable, NullWritable, Writable > {
			
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			//写orc file格式;
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(GroupbyHostRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			row = orcSerde.serialize(new GroupbyHostRow(key.toString().trim()), inspector);
			
			context.write(NullWritable.get(), row);
		}
	}*/
	
	
	private static class DictMapper extends
			Mapper<NullWritable, Writable, Text, NullWritable> {	
		private static List<String[]> DPIList = new ArrayList<String[]>();
		private static final String SCHEMA = "struct<host:string,reportmonth:string>";
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			FileSystem fs = FileSystem.get(conf);
    		String ini="/user/hive/warehouse/label.db/apprules/apprules.txt";
			FSDataInputStream instr = fs.open(new Path(ini));
			BufferedReader buf = new BufferedReader(new InputStreamReader(instr));
			String dpi = null;
			while ((dpi = buf.readLine()) != null) {
				String[] rules = dpi.split(",");
				DPIList.add(rules);
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
            int flag = 0;
            try{
            	String host = inspector.getStructFieldData(struct, inspector.getStructFieldRef("host")).toString().trim();
            	if (host != null && !"".equals(host)){
            		if(host.contains(":")){
		        		host = host.split(":",-1)[0];
		        	}
            		//用apprules规则去匹配
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
    			        outputKey.append(host);
    			        outputKey.append("|");
    			        outputKey.append(appid);
    					context.write(new Text(outputKey.toString()), NullWritable.get());
    				}
            	}
        	}catch(Exception e){};
		}
	}

	/**
	   * MapReduce作业1词频统计的Reducer
	   * 
	   * **/
	private static class DictReducer extends
			Reducer<Text, NullWritable, NullWritable, Writable> {
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			//写orc file格式;
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(host_appRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			row = orcSerde.serialize(new host_appRow(key.toString().trim().split("\\|")), inspector);
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
		/**
	     * 
	     *作业1的配置
	     *生成该月域名:app字典
	     * 
	     * **/
		String month = args[0];
		
		String inputPath = "/user/hive/warehouse/phone.db/host_list/reportmonth=" + month;
		String outputPath = "/user/hadoop/host_app/reportmonth=" + month;
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "host_app");
		job.setJarByClass(host_app.class);
		job.setNumReduceTasks(60);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);

		FileInputFormat.addInputPaths(job, inputPath);
		if (fs.exists(new Path(outputPath))) {
		fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(DictMapper.class);
		job.setReducerClass(DictReducer.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		/**========================================================================*/
	    
	    
		/*String job1_baseInputPath = "/user/hive/warehouse/phone.db/phone_user/reportdate=";
		String job1_inputPath = "";
		String job1_outputPath = "/user/hadoop/host_list/reportmonth=" + month;*/
		/*Job job1 = Job.getInstance(conf, "groupByHost");
		job1.setJarByClass(host_app.class);
		job1.setNumReduceTasks(60);
		job1.setInputFormatClass(OrcNewInputFormat.class);
		job1.setOutputFormatClass(OrcNewOutputFormat.class);
		
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
			
			if (fs.exists(new Path(job1_baseInputPath + dates + "/"))) {
				job1_inputPath = job1_inputPath + job1_baseInputPath + dates + "/,";
			}
			dd.add(Calendar.DATE, 1);//进行当前日期月份加1
		}
		if (fs.exists(new Path(job1_baseInputPath + enddate + "/"))) {
			job1_inputPath = job1_inputPath + job1_baseInputPath + enddate + "/";
		}else{
			job1_inputPath = job1_inputPath.substring(0, job1_inputPath.length()-1);
		}
		
		FileInputFormat.addInputPaths(job1, job1_inputPath);
		if (fs.exists(new Path(job1_outputPath))) {
		fs.delete(new Path(job1_outputPath), true);
		}
		FileOutputFormat.setOutputPath(job1, new Path(job1_outputPath));
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Writable.class);
		job1.setMapperClass(GroupbyMapper.class);
		job1.setReducerClass(GroupbyReducer.class);
		job1.waitForCompletion(true);*/
	}
}
