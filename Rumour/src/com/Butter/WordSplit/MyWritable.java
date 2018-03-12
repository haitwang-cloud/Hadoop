package com.Butter.WordSplit;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class MyWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES = null;

	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] {
				IntWritable.class, Text.class };
	}

	public MyWritable() {
		
	}
	
	public MyWritable(Writable instance) {
		this.set(instance);
	}
	
	@Override
	protected Class<? extends Writable>[] getTypes() {
		// TODO Auto-generated method stub
		return CLASSES;
	}

}
