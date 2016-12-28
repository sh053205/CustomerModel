package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class UserLabelRow implements Writable {
	String msisdn;
	String imei;
	String userlabel;
	String appname;
	String apptype;
	String count;
	
	UserLabelRow(String[] val){
		this.msisdn = val[0];
		this.imei = val[1];
		this.userlabel = val[2];
		this.appname = val[3];
		this.apptype = val[4];
		this.count = val[5];
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