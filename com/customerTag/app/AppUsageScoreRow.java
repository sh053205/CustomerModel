package com.customerTag.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AppUsageScoreRow implements Writable {
	String msisdn;
	String appid;
	double score;
	
	AppUsageScoreRow(String[] val){
		this.msisdn = val[0];
		this.appid = val[1];
		this.score = Double.parseDouble(val[2]);
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