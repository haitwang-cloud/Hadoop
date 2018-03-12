package com.Butter.WordUnionFrequence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordUnionFrequencePartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// 根据Hash值均匀分区
		return ((key.hashCode() & Integer.MAX_VALUE) % numPartitions);
	}

}