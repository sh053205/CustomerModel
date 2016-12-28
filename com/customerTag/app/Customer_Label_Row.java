package com.customerTag.app;
/*
 * AppPrefs.java写orc文件格式需要使用该类进行格式化*/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Customer_Label_Row implements Writable {
	String msisdn;
	String type;
	String label;
	double score;
	
	Customer_Label_Row(String[] val){
		this.msisdn = val[0];
		this.type = val[1];
		this.label = val[2];
		this.score = Double.parseDouble(val[3]);
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