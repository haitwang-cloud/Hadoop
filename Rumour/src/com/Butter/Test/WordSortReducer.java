package com.Butter.Test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordSortReducer extends
		Reducer<IntWritable, Text, Text, IntWritable> {
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Text t : values) {
			context.write(t, key);
		}
	}
	
}