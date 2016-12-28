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

import test.AppPrefsRow_host_uri;

public class AppPrefs_MapOnly {
	private static class ExtractorMapper extends
			Mapper<NullWritable, Writable, NullWritable, Writable> {
		private static final String TAB = "|";
		private static List<String[]> DPIList = new ArrayList<String[]>();
		private static final String SCHEMA = "struct<dpi_from:string,imsi:string,imei:string,msisdn:string,begin_time:string,end_time:string,prot_category:string,prot_type:string,host:string,fst_uri:string,user_agent:string,rat:string,server_ip:string,server_port:string,l4_ul_throughput:float,l4_dw_throughput:float,lac:string,rac:string,ci:string,sac:string,eci:string,air_port_duration:float,reportdate:string,hour:int>";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
		}

		@Override
		protected void map(
				NullWritable key, 
				Writable value,
				Mapper<NullWritable, Writable, NullWritable, Writable>.Context context)
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
                if(msisdn != null && !"".equals(msisdn) && host != null && !"".equals(host)){
                	if(host.contains(":")){
                		host = host.split(":",-1)[0];
                	}
                	if(host.equals("m.ctrip.com") && fst_uri.contains("/html5/flight/")){
                		String[] result = {msisdn, imei};
        	    		
        	    		OrcSerde orcSerde = new OrcSerde();
        	    		Writable row;
        				StructObjectInspector inspectorout = 
        						(StructObjectInspector) ObjectInspectorFactory
        						.getReflectionObjectInspector(AppPrefsRow_host_uri.class,
        								ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        				//String[] keys = key.toString().trim().split("\\|");
        				//String[] result = new String[6];
        				//System.arraycopy(keys, 0, result, 0, keys.length);
        				//result[5] = Integer.toString(sum);
        				row = orcSerde.serialize(new AppPrefsRow_host_uri(result), inspectorout);
        	    		
    					context.write(NullWritable.get(),row);
                	}
                }
            }catch(Exception e){};
		}
	}

	/*private static class ExtractorReducer extends
			Reducer<Text, NullWritable, NullWritable, Writable > {		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			//int sum = 0;
			//写orc file格式;
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(AppPrefsRow_host_uri.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			String[] keys = key.toString().trim().split("\\|");
			//String[] result = new String[6];
			//System.arraycopy(keys, 0, result, 0, keys.length);
			//result[5] = Integer.toString(sum);
			row = orcSerde.serialize(new AppPrefsRow_host_uri(keys), inspector);
			context.write(NullWritable.get(), row);
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
		Job job = Job.getInstance(conf, "IOTEST");
		job.setJarByClass(AppPrefs_MapOnly.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		
		FileInputFormat.addInputPaths(job, inputPath);

		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
		//MultipleOutputs.addNamedOutput(job,"AppPrefs_host_uri",OrcNewOutputFormat.class,NullWritable.class,Writable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Writable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(ExtractorMapper.class);
		//job.setReducerClass(ExtractorReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
