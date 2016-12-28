package com.customerTag.app;
/*
 * AppPrefs.java写orc文件格式需要使用该类进行格式化*/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AppPrefsRow implements Writable {
	String msisdn;
	String appid;
	String month;
	String hoursPerDay;
	String daysThisMonth;
	String hoursThisMonth;
	String timesThisMonth;
	
	AppPrefsRow(String[] val){
		this.msisdn = val[0];
		this.appid = val[1];
		this.month = val[2];
		this.hoursPerDay = val[3];
		this.daysThisMonth = val[4];
		this.hoursThisMonth = val[5];
		this.timesThisMonth = val[6];
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