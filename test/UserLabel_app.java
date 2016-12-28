package test;
/*客户app使用情况统计，包括的统计数据为：
 * 1.该月每天平均使用小时数；
 * 2.该月使用天数*/
import java.io.BufferedReader;
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
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

import com.customerTag.app.TextArrayWritable;

public class UserLabel_app {
	private static class ExtractorMapper extends
			Mapper<NullWritable, Writable, Text, IntWritable> {
		private static final String TAB = "|";
		//private static List<String[]> DPIList = new ArrayList<String[]>();
		private static final String SCHEMA = "struct<dpi_from:string,imsi:string,imei:string,msisdn:string,begin_time:string,end_time:string,prot_category:string,prot_type:string,host:string,fst_uri:string,user_agent:string,rat:string,server_ip:string,server_port:string,l4_ul_throughput:float,l4_dw_throughput:float,lac:string,rac:string,ci:string,sac:string,eci:string,air_port_duration:float,reportdate:string,hour:int>";
		private static String[] DPIArray = new String[]{};
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			//读取hdfs上的appRules库
			Configuration conf = context.getConfiguration();
			TextArrayWritable dpi=DefaultStringifier.load(conf, "dpi", TextArrayWritable.class);
			DPIArray = dpi.toStrings();
			/*for(int i = 0; i<dpis.length; i++){
				DPIArray[i] = dpis[i].toString();
			}*/
		}

		@Override
		protected void map(
				NullWritable key, 
				Writable value,
				Mapper<NullWritable, Writable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			OrcStruct struct = (OrcStruct)value;
			TypeInfo typeInfo =
                    TypeInfoUtils.getTypeInfoFromTypeString(SCHEMA);
            
            StructObjectInspector inspector = (StructObjectInspector)
                    OrcStruct.createObjectInspector(typeInfo);
            
           try{
        	    String imei = inspector.getStructFieldData(struct, inspector.getStructFieldRef("imei")).toString().trim();
            	String msisdn = inspector.getStructFieldData(struct, inspector.getStructFieldRef("msisdn")).toString().trim();
                String host = inspector.getStructFieldData(struct, inspector.getStructFieldRef("host")).toString().trim();
                String fst_uri = inspector.getStructFieldData(struct, inspector.getStructFieldRef("fst_uri")).toString().trim();
				
				//进行应用匹配且写入context;
                if (msisdn != null && !"".equals(msisdn) && host != null && !"".equals(host)){
                	if(host.contains(":")){
                		host = host.split(":",-1)[0];
                	}
    				for(String val:DPIArray){
    					String[] dpi = val.trim().split(",");
    					String host_app = dpi[3];
    					String uri = dpi[4];
            			if(host.equals(host_app) && fst_uri.contains(uri)){
	        	    		StringBuffer outputKey = new StringBuffer();
	        	    		outputKey.append(msisdn);
	        	    		outputKey.append(TAB);
	        	    		outputKey.append(imei);
	        	    		outputKey.append(TAB);
	        	    		outputKey.append(dpi[0]);
	        	    		outputKey.append(TAB);
	        	    		outputKey.append(dpi[1]);
	        	    		outputKey.append(TAB);
	        	    		outputKey.append(dpi[2]);
	    					context.write(new Text(outputKey.toString()),new IntWritable(1));
	        	    	}
                	}
                }
            }catch(Exception e){};
		}
	}

	private static class ExtractorReducer extends
			Reducer<Text, IntWritable, NullWritable, Writable > {		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			//写orc file格式;
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(UserLabelRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			String[] keys = key.toString().trim().split("\\|");
			String[] result = new String[6];
			System.arraycopy(keys, 0, result, 0, keys.length);
			for(IntWritable val:values){
				sum = sum + val.get();
			}
			result[5] = Integer.toString(sum);
			row = orcSerde.serialize(new UserLabelRow(result), inspector);
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

		ArrayList<String> DPI=new ArrayList<String>();
		String inputPath = "";
		String baseInputPath = "";
		if(args[0].endsWith("/")){
			baseInputPath = args[0]+"reportdate=";
		}else{
			baseInputPath = args[0]+"/reportdate=";
		}
		String reportdate = args[1];
		String outputPath = "";
		if(args[2].endsWith("/")){
			outputPath = args[2]+"reportdate="+reportdate;
		}else{
			outputPath = args[2]+"/reportdate="+reportdate;
		}
		
		Configuration conf = new Configuration();
		String ini="/user/hadoop/hostUriRules.txt";
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(ini));
		BufferedReader bf = new BufferedReader(new InputStreamReader(in));
		String str = null;
		while ((str = bf.readLine()) != null) {
			DPI.add(str);
		}
		if (bf != null) {
			bf.close();
		}
		String[] dpis = new String[DPI.size()];
		DPI.toArray(dpis);
		DefaultStringifier.store(conf,new TextArrayWritable(dpis),"dpi");
		
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "UserLabel_app");
		job.setJarByClass(UserLabel_app.class);
		//job.setNumReduceTasks(40);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		inputPath = baseInputPath + reportdate + "/";
		FileInputFormat.addInputPaths(job, inputPath);

		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
		//MultipleOutputs.addNamedOutput(job,"AppPrefs_host_uri",OrcNewOutputFormat.class,NullWritable.class,Writable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(ExtractorMapper.class);
		job.setReducerClass(ExtractorReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
