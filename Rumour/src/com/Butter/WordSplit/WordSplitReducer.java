package com.Butter.WordSplit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordSplitReducer extends
		Reducer<Text, DataValue, WritableComparable, Writable> {

	private final static Text splitTextData = new Text("<splitTextData>");

	private MultipleOutputs mos = null;

	private IntWritable result = new IntWritable();

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		mos.close();
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		mos = new MultipleOutputs(context);
	}

	@Override
	protected void reduce(Text key, Iterable<DataValue> values, Context context)
			throws IOException, InterruptedException {
		// 首先判断是否是splitTextData，如果是则直接输出分词结果，否则就统计词频
		if (!key.toString().equals(splitTextData.toString())) {
			int sum = 0;
			for (DataValue val : values) {
				sum += val.getIntData().get();
			}
			result.set(sum);
			// 词频统计结果输出到wordFrequence文件夹下面
			mos.write("wordFrequence", key, result, "wordFrequence/");
		} else {
			// 分词结果输出到wordSplitData文件夹下面
			for (DataValue val : values) {
				mos.write("dataWithSplit", val.getTextData(),
						NullWritable.get(), "wordSplitData/");
			}
		}
	}
	
}