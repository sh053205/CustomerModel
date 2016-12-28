package com.customerTag.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class host_appRow implements Writable {
	String msisdn;
	String appid;
	
	host_appRow(String[] val){
		this.msisdn = val[0];
		this.appid = val[1];
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