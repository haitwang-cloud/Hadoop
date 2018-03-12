package com.Butter.WordUnionFrequence;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordUnionFrequenceJob extends Configured implements Tool {

	/**
	 * 联合词频统计Job 统计词语a,b联合出现的次数 
	 * args[0]:reduce个数
	 * args[1]:该job数据输入路径（即第一个job对每条微博所分词后的文件保存路径） 
	 * args[2]:该job数据输出路径
	 * args[3]:上一个job数据输出路径（即排序后的结果） 
	 * args[4]:需要聚类的词语个数
	 */
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 5) {
			System.err.println("Parameter error");
			System.exit(2);
		}

		Job job = new Job();
		// 设置Job名字
		job.setJobName("WordUnionFrequenceJob");
		// 设置Reduce个数
		job.setNumReduceTasks(Integer.parseInt(args[0]));
		job.setJarByClass(WordUnionFrequenceJob.class);
		// 设置Map类
		job.setMapperClass(WordUnionFrequenceMapper.class);
		// 设置Reduce类
		job.setReducerClass(WordUnionFrequenceReducer.class);
		// 设置分区类
		job.setPartitionerClass(WordUnionFrequencePartitioner.class);
		// 设置map输出时key的类型
		job.setMapOutputKeyClass(Text.class);
		// 设置map输出时value的类型
		job.setMapOutputValueClass(IntWritable.class);
		// 设置reduce输出时key的类型
		job.setOutputKeyClass(Text.class);
		// 设置reduce输出时value的类型
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// 在Job启动之前，对上一个Job排序后的词频文件取前X个，X的个数由args[4]决定，并将取出后的前X个词语分发到各个TaskNode目录下，统计这些词语俩俩之间同时出现的次数
		new WordFrequenceFileUtil().distributedWordFrequenceFile(args[3],
				Integer.parseInt(args[4]), job.getConfiguration());

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}