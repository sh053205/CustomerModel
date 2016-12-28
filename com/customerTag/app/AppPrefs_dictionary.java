package com.customerTag.app;
/*用户号码匹配appID工具，该工具先匹配历史保存的 host-appid字典，
 *若无记录，再去使用apprules进行msisdn-appid匹配，并且将结果保存至dictionary
 * 该方法目前已弃用，因为直接进行apprules效率合格，而且字典容易带来内存问题*/
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

public class AppPrefs_dictionary {
	
	private static final String TAB = "|";
	private static final String SCHEMA = "struct<host:string,reportmonth:string>";
	/**
	   * MapReduce作业1词频统计的Mapper
	   * 
	   * **/
	private static class DicsMapper extends
			Mapper<NullWritable, Writable, Text, NullWritable> {
		private static HashMap<String,String> DictMap = new HashMap<String,String>();
		private static List<String[]> DPIList = new ArrayList<String[]>();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			String ini="/user/hadoop/AppDictionary/";
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(ini));
			if(status.length>0){
				for(FileStatus file:status){
					FSDataInputStream in = fs.open(file.getPath());
					BufferedReader bf = new BufferedReader(new InputStreamReader(in));
					String str = null;
					while ((str = bf.readLine()) != null) {
						String[] rules = str.split(",");
						DictMap.put(rules[0], rules[1]);
					}
					if (bf != null) {
						bf.close();
					}
				}
			}
						
    		ini="/user/hive/warehouse/label.db/apprules/apprules.txt";
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
            	StringBuffer outputKey = new StringBuffer();
            	String host = inspector.getStructFieldData(struct, inspector.getStructFieldRef("host")).toString().trim();
            	if (host != null && !"".equals(host)){
            		if(host.contains(":")){
                		host = host.split(":",-1)[0];
                	}
                	
                	if(!DictMap.isEmpty()){
                		String appid = new String();
                		if((appid = DictMap.get(host)) != null){
                			flag = 1;
                			outputKey.append(host);
                			outputKey.append(",");
                			outputKey.append(appid);
                			context.write(new Text(outputKey.toString()), NullWritable.get());
                		}
                	}
                	
                	if(flag == 0){
                		//用apprules规则去匹配
                		int labelPrefs = 0;//优选label为0的标签;
                    	String appid = new String();
                    	String appname = new String();
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
        		        	    		appname = dpi[2];
        	        	    		}
        	        	    	}
                    		}else if(label == 1 && labelPrefs == 0){
                    			if(host.endsWith(host1) || host.equals(host2)){
                    				flag = 1;
                    				//避免匹配到重复项
                    				if(host1.length()>len1){
        	        	    			len1 = host1.length();
        		        	    		appid = dpi[0];
        		        	    		appname = dpi[2];
        	        	    		}
                    			}
                    		}
                    	}
        				
        				SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        				String hdfs_path = "/user/hadoop/AppDictionary/"+df.format(new Date());//文件路径  
    			        Configuration conf = new Configuration();
    			        FileSystem fs = FileSystem.get(conf);
        				//FSDataOutputStream fout = fs.create(new Path(hdfs_path)); 
        				//BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
    			        FSDataOutputStream fout = null;
    			        if(fs.exists(new Path(hdfs_path))){
    			        	fout = fs.append(new Path(hdfs_path));
    			        }else{
    			        	fout = fs.create(new Path(hdfs_path)); 
    			        }
    			        
    			        StringBuffer strBuf = new StringBuffer();
    			        			        
        				if(flag == 1){
        					strBuf.append(host);
        			        strBuf.append(",");
        			        strBuf.append(appid);
        			        strBuf.append(",");
        			        strBuf.append(appname);
        			        outputKey.append(host);
        			        outputKey.append(",");
        			        outputKey.append(appid);
        					fout.write(strBuf.toString().getBytes());
        					context.write(new Text(outputKey.toString()), NullWritable.get());
        				}else{
        					strBuf.append(host);
        			        strBuf.append(",none,none");
        			        fout.write(strBuf.toString().getBytes());
        				}
                	}
            	}
        	}catch(Exception e){};
		}
	}

	/**
	   * MapReduce作业1词频统计的Reducer
	   * 
	   * **/
	private static class DicsReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
				context.write(key, NullWritable.get());
		}
	}
	
	/*private static class AppMapper extends
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

	private static class AppReducer extends
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
	}*/

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

		String inputPath = args[0];
		String outputPath = args[1];
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "App Preferences Dictionary");
		job.setJarByClass(AppPrefs_dictionary.class);
		job.setNumReduceTasks(60);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPaths(job, inputPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
		fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(DicsMapper.class);
		job.setReducerClass(DicsReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
