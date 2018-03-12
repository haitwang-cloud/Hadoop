package com.Butter.WordSplit;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordSplitJob extends Configured implements Tool {

	/**
	 * 单独测试用
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException, URISyntaxException {

		if (args.length != 3) {
			System.err.println("Parameter error");
			System.exit(2);
		}

		Job job = new Job();
		job.setJobName("WordSplitJob");

		job.setNumReduceTasks(Integer.parseInt(args[0]));
		job.setJarByClass(WordSplitJob.class);
		job.setMapperClass(WordSplitMapper.class);
		job.setReducerClass(WordSplitReducer.class);
		job.setPartitionerClass(WordSplitPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataValue.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		/*
		 * DistributedCache.createSymlink(job.getConfiguration()); URI
		 * ictclasURI = new
		 * URI("/user/butter/ictclas/libICTCLAS50.so#libICTCLAS50.so");
		 * DistributedCache.addCacheFile(ictclasURI, job.getConfiguration());
		 */

		MultipleOutputs.addNamedOutput(job, "wordFrequence",
				TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "dataWithSplit",
				TextOutputFormat.class, Text.class, NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * 分词Job
	 * 
	 */
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.err.println("Parameter error");
			System.exit(2);
		}

		Job job = new Job();
		// 设置Job名字
		job.setJobName("WordSplitJob");
		// 设置reduce个数
		job.setNumReduceTasks(Integer.parseInt(args[0]));
		job.setJarByClass(WordSplitJob.class);
		// 设置Map类
		job.setMapperClass(WordSplitMapper.class);
		// 设置Reducer类
		job.setReducerClass(WordSplitReducer.class);
		// 设置分区类
		job.setPartitionerClass(WordSplitPartitioner.class);
		// 设置Map输出时KEY的类型
		job.setMapOutputKeyClass(Text.class);
		// 对应Value的类型
		job.setMapOutputValueClass(DataValue.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// 分词系统需要用到libICTCLAS50.so这个库文件，这里将在分布式系统上的库文件分发到各个TaskNode结点下
		DistributedCache.createSymlink(job.getConfiguration());
		URI ictclasURI = new URI(
				"/user/butter/ictclas/libICTCLAS50.so#libICTCLAS50.so");
		DistributedCache.addCacheFile(ictclasURI, job.getConfiguration());

		// 设置多文件输出,Reducer需要输出两种结果，一种为词频统计结果，另一种为分词结果
		MultipleOutputs.addNamedOutput(job, "wordFrequence",
				TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "dataWithSplit",
				TextOutputFormat.class, Text.class, NullWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}