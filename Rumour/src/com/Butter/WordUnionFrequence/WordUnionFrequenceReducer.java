package com.Butter.WordUnionFrequence;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordUnionFrequenceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{	
	
	private IntWritable result = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// 统计联合词频
		int sum = 0;
		for (IntWritable val : values)
				sum += val.get();
		result.set(sum);
		// 输出<"p1,p2",出现的次数>
		context.write(key, result);
	}
	
}