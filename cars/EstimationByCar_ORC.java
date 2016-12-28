/*最终版汽车拥有情况分析工具*/
package cars;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class EstimationByCar_ORC {
	private static class ExtractorMapper extends
			Mapper<NullWritable, Writable, Text, NullWritable> {
		private static final String TAB = "|";
		private static List<String[]> keyInfoList = new ArrayList<String[]>();
		private static final String SCHEMA = "struct<sn:string,user_type:string,msisdn_smo:string,ec_code:string,msisdn_smt:string,service_code:string,operate_code:string,home:string,gateway:string,begin_time:string,reportdate:string>";
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			//读取hdfs上的dpi库
			Configuration conf = context.getConfiguration();
			String ini="/user/hadoop/customersModel/car.txt";
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(ini));
			BufferedReader bf = new BufferedReader(new InputStreamReader(in));
			String str = null;
			while ((str = bf.readLine()) != null) {
				String[] rules = str.split("\\|\\|");
				keyInfoList.add(rules);
			}
			if (bf != null) {
				bf.close();
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
            	String msisdn_smt = inspector.getStructFieldData(struct, inspector.getStructFieldRef("msisdn_smt")).toString().trim();
                String service_code = inspector.getStructFieldData(struct, inspector.getStructFieldRef("service_code")).toString().trim();
                String reportdate = inspector.getStructFieldData(struct, inspector.getStructFieldRef("begin_time")).toString().trim().substring(0, 8);
                
                if (msisdn_smt != null && !"".equals(msisdn_smt) && service_code != null && !"".equals(service_code)){
                	for (String[] websiteInfo : keyInfoList){
                		String label = websiteInfo[0];
                		String servCode = websiteInfo[2].trim();
                		if(label.trim().equals("0")){
                    		if(service_code.equals(servCode)){
                    			StringBuilder outputKey = new StringBuilder();
    							outputKey.append(msisdn_smt);
    							outputKey.append(TAB);
    							outputKey.append(websiteInfo[3]);
    							outputKey.append(TAB);
    							outputKey.append(websiteInfo[1]);
    							outputKey.append(TAB);
    							outputKey.append(service_code);
    							outputKey.append(TAB);
    							outputKey.append(reportdate);
    							outputKey.append(TAB);
    							outputKey.append(websiteInfo[5]);
    							context.write(new Text(outputKey.toString()),NullWritable.get());
        						break;
                    		}
                		}else{
                			if(service_code.endsWith(servCode)){
                				StringBuilder outputKey = new StringBuilder();
    							outputKey.append(msisdn_smt);
    							outputKey.append(TAB);
    							outputKey.append(websiteInfo[3]);
    							outputKey.append(TAB);
    							outputKey.append(websiteInfo[1]);
    							outputKey.append(TAB);
    							outputKey.append(service_code);
    							outputKey.append(TAB);
    							outputKey.append(reportdate);
    							outputKey.append(TAB);
    							outputKey.append(websiteInfo[5]);
    							context.write(new Text(outputKey.toString()),NullWritable.get());
    						break;
                			}
                		}
                	}
                }
            }catch(Exception e){};
		}
	}

	private static class ExtractorReducer extends
			Reducer<Text, NullWritable, NullWritable, Writable> {

		//private final OrcSerde orcSerde = new OrcSerde();

		//private List<Text> orcRecord;
		//private Writable row;
		//private final String testStruct = "struct<msisdn_smt:string,type:string,name:string,servcode:string,reportdate:string,score:string>";
		//private final TypeInfo personTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(testStruct);
		//private final ObjectInspector personOip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(personTypeInfo);
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			//Properties table = new Properties();
	        //table.setProperty("columns", "a,b,c");
	        //table.setProperty("columns.types", "int,string,struct<d:int,e:string>");
	        
			//this.orcSerde.initialize(job, table);
			//this.orcRecord = new ArrayList<Text>();
		    //this.orcRecord.add(key);
			//写orc方法1：
			OrcSerde orcSerde = new OrcSerde();
			Writable row;
			String[] val = key.toString().trim().split("\\|");
			//StringTokenizer val = new StringTokenizer();
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(MyRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

				row = orcSerde.serialize(new MyRow(val), inspector);
				context.write(NullWritable.get(), row);
		}
	}

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			URISyntaxException, InterruptedException, ClassNotFoundException {

		String inputPath = args[0];
		String outputPath = args[1];

		Configuration conf = new Configuration();
		conf.set("mapreduce.job.queuename", "background");
		Job job = Job.getInstance(conf, "EstimationByCar_ORC");
		job.setJarByClass(EstimationByCar_ORC.class);
		job.setNumReduceTasks(40);
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
		fs.delete(new Path(outputPath), true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setMapperClass(ExtractorMapper.class);
		job.setReducerClass(ExtractorReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
