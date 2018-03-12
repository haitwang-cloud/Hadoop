package com.Butter.Test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.Butter.WordSplit.WordSplitJob;
import com.Butter.WordSplit.WordSplitMapper;
import com.Butter.WordSplit.WordSplitPartitioner;
import com.Butter.WordSplit.WordSplitReducer;

public class WordSort {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Parameter error");
			System.exit(2);
		}
		
		Job job = new Job(conf, "WordSort");
		job.setNumReduceTasks(Integer.valueOf(args[0]));
		job.setJarByClass(WordSort.class);
		job.setMapperClass(WordSortMapper.class);
		job.setReducerClass(WordSortReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
