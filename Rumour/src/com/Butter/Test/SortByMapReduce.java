package com.Butter.Test;

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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new SortByMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = (JobConf) getConf();

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setJobName("SortByMapReduce");
		conf.setJarByClass(SortByMapReduce.class);

		conf.setInputFormat(CxfInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setNumReduceTasks(2);

		conf.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.RandomSampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(
				0.1, 5, 2);

		Path inputDir = new Path(args[0]);
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
		Path partitionFile = new Path(inputDir, "_partitions");

		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(conf, sampler);

		URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);

		conf.setInt("dfs.replication", 1);
		JobClient.runJob(conf);
		return 0;
	}
}