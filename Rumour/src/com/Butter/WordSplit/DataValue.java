package com.Butter.WordSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DataValue implements WritableComparable<DataValue>{

	private BooleanWritable type = null;
	private IntWritable intData = null;
	private Text textData = null;
	
	public DataValue() {
		type = new BooleanWritable();
	}
	
	public DataValue(IntWritable intData) {
		this.intData = intData;
		this.textData = null;
		type.set(true);
	}
	
	public DataValue(Text textData) {
		this.textData = textData;
		this.intData = null;
		type.set(false);
	}
	
	public void set(IntWritable intData) {
		this.intData = intData;
		this.textData = null;
		type.set(true);
	}
	
	public void set(Text textData) {
		this.textData = textData;
		this.intData = null;
		type.set(false);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		type.readFields(in);
		if (type.get()) {
			intData = new IntWritable();
			intData.readFields(in);
		} else {
			textData = new Text();
			textData.readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		type.write(out);
		if (type.get())
			intData.write(out);
		else
			textData.write(out);
	}

	@Override
	public int compareTo(DataValue o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		if (type.get())
			return intData.toString();
		else
			return textData.toString();
	}

	public IntWritable getIntData() {
		return intData;
	}

	public void setIntData(IntWritable intData) {
		this.intData = intData;
	}

	public Text getTextData() {
		return textData;
	}

	public void setTextData(Text textData) {
		this.textData = textData;
	}

}