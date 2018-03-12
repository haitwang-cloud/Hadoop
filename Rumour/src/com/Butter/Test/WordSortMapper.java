package com.Butter.Test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordSortMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private IntWritable result = new IntWritable();

	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] values = value.toString().split("	");
		int num = Integer.valueOf(values[1]);
		result.set(num);
		word.set(values[0]);
		context.write(result, word);
	}

}
