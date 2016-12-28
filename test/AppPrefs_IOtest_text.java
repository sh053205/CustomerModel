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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class AppPrefs_IOtest_text {
	private static class ExtractorMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static final String TAB = "|";
		private static List<String[]> DPIList = new ArrayList<String[]>();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
		}

		@Override
		protected void map(
				LongWritable key, 
				Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			if(line.length() > 0){
				String[] columns = line.split(TAB);
				String imei = columns[2];
				String msisdn = columns[3];
				String host = columns[8];
				String fst_uri = columns[9];
				if(msisdn != null && !"".equals(msisdn) && host != null && !"".equals(host)){
                	if(host.contains(":")){
                		host = host.split(":",-1)[0];
                	}
                	if(host.equals("m.ctrip.com") && fst_uri.contains("/html5/flight/")){
                		StringBuffer outputKey = new StringBuffer();
        	    		outputKey.append(msisdn);
        	    		outputKey.append(TAB);
        	    		outputKey.append(imei);
    					context.write(new Text(outputKey.toString()),new IntWritable(1));
                	}
                }
			}
		}
	}

	private static class ExtractorReducer extends
			Reducer<Text, IntWritable, NullWritable, Writable > {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//写orc file格式;
			
			context.write(NullWritable.get(), key);
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

		String inputPath = args[0];
		String outputPath = args[1];
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "IOTEST");
		job.setJarByClass(AppPrefs_IOtest_text.class);
		job.setNumReduceTasks(40);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		
		FileInputFormat.addInputPaths(job, inputPath);

		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
		//MultipleOutputs.addNamedOutput(job,"AppPrefs_host_uri",OrcNewOutputFormat.class,NullWritable.class,Writable.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ExtractorMapper.class);
		job.setReducerClass(ExtractorReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
