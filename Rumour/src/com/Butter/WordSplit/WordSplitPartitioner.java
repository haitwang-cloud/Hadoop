package com.Butter.WordSplit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordSplitPartitioner extends Partitioner<Text, DataValue> {

	private final static Text splitTextData = new Text("<splitTextData>");

	@Override
	public int getPartition(Text key, DataValue value, int numPartitions) {
		// 根据Hash计算分区，负载平衡
		if (!key.toString().equals(splitTextData.toString())) {
			return ((key.hashCode() & Integer.MAX_VALUE) % numPartitions);
		} else {
			return ((value.getTextData().hashCode() & Integer.MAX_VALUE) % numPartitions);
		}
	}

}