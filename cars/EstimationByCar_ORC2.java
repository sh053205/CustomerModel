package cars;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import cars.TestOrcWriter.MyRow;

public class EstimationByCar_ORC2 {
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
        							StringBuffer outputKey = new StringBuffer();
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
    							StringBuffer outputKey = new StringBuffer();
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
			Reducer<Text, Text, NullWritable, Writable> {

		private final OrcSerde orcSerde = new OrcSerde();

		//private List<String> orcRecord;
		private Writable row;
		//private final String testStruct = "struct<msisdn_smt:string,type:string,name:string,servcode:string,reportdate:string,score:string>";
		//private final TypeInfo personTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(testStruct);
		//private final ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(personTypeInfo);
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//写orc方法2主要区别在于如何生成objectInspector，可以重写abstractSerDe类，生成匹配的辅助类objectInspector，或者使用ObjectInspectorFactory生成objectinspector进行序列化，具体做法如下：
			String[] val = key.toString().trim().split("\\|");
			Configuration conf = context.getConfiguration();
			Properties table = new Properties();
	        table.setProperty("columns", "msisdn_smt,type,name,servcode,reportdate,score");
	        table.setProperty("columns.types", "string,string,string,string,string,string");
	        
			//this.orcSerde.initialize(conf, table);
	        String columnNameProp = table.getProperty(Constants.LIST_COLUMNS);
	        List<String> columnNames = Arrays.asList(columnNameProp.split(","));
	        String columnTypeProp = table.getProperty(Constants.LIST_COLUMN_TYPES);
	        ArrayList<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProp);
	        
	        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
	        ObjectInspector oi;
	        for (int c = 0; c < columnNames.size(); c++) {
	        oi = TypeInfoUtils
	        .getStandardJavaObjectInspectorFromTypeInfo(columnTypes
	        .get(c));
	        columnOIs.add(oi);
	        }
			StructObjectInspector inspector = (StructObjectInspector) ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
			
			/*this.orcRecord = new ArrayList<String>();
			for(int i=0;i<val.length;i++){
				this.orcRecord.add(val[i]);
			}*/
		    this.row = orcSerde.serialize(new MyRow(val), inspector);
			//写orc方法1：
			/*String[] val = key.toString().trim().split("\\|");
			StructObjectInspector inspector = 
					(StructObjectInspector) ObjectInspectorFactory
					.getReflectionObjectInspector(MyRow.class,
							ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		    
		    this.row = orcSerde.serialize(new MyRow(val), inspector);*/
			context.write(NullWritable.get(), this.row);
		}
		
		static class MyRow implements Writable {
			String msisdn_smt;
			String type;
			String name;
			String servcode;
			String reportdate;
			String score;
			
			MyRow(String[] val){
				this.msisdn_smt = val[0];
				this.type = val[1];
				this.name = val[2];
				this.servcode = val[3];
				this.reportdate = val[4];
				this.score = val[5];
				
			}
			@Override
			public void readFields(DataInput arg0) throws IOException {
				throw new UnsupportedOperationException("no write");
			}
			@Override
			public void write(DataOutput arg0) throws IOException {
				throw new UnsupportedOperationException("no read");
			}
			
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
		Job job = Job.getInstance(conf, "EstimationByCar");
		job.setJarByClass(EstimationByCar_ORC2.class);
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
		job.setOutputKeyClass(Writable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(ExtractorMapper.class);
		job.setReducerClass(ExtractorReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
