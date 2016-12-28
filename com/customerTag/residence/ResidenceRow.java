package com.customerTag.residence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ResidenceRow implements Writable {
	String msisdn;
	String lac;
	String ci;
	String type;
	float score;
	
	ResidenceRow(String[] val){
		this.msisdn = val[0];
		this.lac = val[1];
		this.ci = val[2];
		this.type = val[3];
		this.score = Float.parseFloat(val[4]);
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