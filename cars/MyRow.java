package cars;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class MyRow implements Writable {
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