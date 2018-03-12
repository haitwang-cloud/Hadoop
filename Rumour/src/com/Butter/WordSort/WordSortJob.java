package com.Butter.WordSort;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordSortJob extends Configured implements Tool {

	public static void main(String[] args)  throws Exception {
		int res = ToolRunner.run(new JobConf(), new WordSortJob(), args);
		System.exit(res);
	}

	/**
	 * 词频排序Job
	 */
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = (JobConf) getConf();

		// 设置reduce个数
		conf.setNumReduceTasks(Integer.parseInt(args[0]));
		// 设置输入路径
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		// 设置输出路径
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		// 设置Job名字
		conf.setJobName("WordSortJob");
		conf.setJarByClass(WordSortJob.class);

		// 设置读取格式化类
		conf.setInputFormat(WordSortInputFormat.class);
		// 设置key的类型
		conf.setOutputKeyClass(LongWritable.class);
		// 设置value的类型
		conf.setOutputValueClass(Text.class);
		// 设置分区类
		conf.setPartitionerClass(TotalOrderPartitioner.class);
		// 创建采样类
		InputSampler.RandomSampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(
				0.1, 10000, 10);
		// 设置分区文件保存路径
		Path inputDir = new Path(args[1]);
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
		Path partitionFile = new Path(inputDir, "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		// 对词频文件进行采样，因为词频文件的分布成正态分布，分布并不均匀，需要采样，并将采样后的结果排序后保存到_partitions中
		// 比如设2个reduce，对100个数据进行取样,[负无穷,30]范围内的数据输出到第一个reduce中,[30,正无穷]范围内的数据输出到第二个reduce中
		InputSampler.writePartitionFile(conf, sampler);
		// 将分区文件分发到各个TaskNode目录下
		URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);
		// 设置比较器，将默认按递增的顺序改为按递减的顺序
		conf.setOutputKeyComparatorClass(LongComparator.class);
		
		conf.setInt("dfs.replication", 1);
		// 执行Job
		JobClient.runJob(conf);
		
		return 0;
	}

}