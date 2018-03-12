package com.Butter.WordSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class WordSortInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		return new WordSortRecordReader(conf, (FileSplit) split);
	}

	/**
	 * 内部类，格式化输入流
	 * 
	 * @author butter
	 * 
	 */
	class WordSortRecordReader implements RecordReader<LongWritable, Text> {

		private LineRecordReader in;
		private LongWritable junk = new LongWritable();
		private Text line = new Text();

		public WordSortRecordReader(JobConf job, FileSplit split)
				throws IOException {
			in = new LineRecordReader(job, split);
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			in.close();
		}

		@Override
		public LongWritable createKey() {
			// TODO Auto-generated method stub
			return new LongWritable();
		}

		@Override
		public Text createValue() {
			// TODO Auto-generated method stub
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			// TODO Auto-generated method stub
			return in.getPos();
		}

		@Override
		public float getProgress() throws IOException {
			// TODO Auto-generated method stub
			return in.getProgress();
		}

		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			// TODO Auto-generated method stub
			if (in.next(junk, line)) {
				String temp = line.toString();
				String[] data = temp.split("	");
				if (data.length == 2) {
					key.set(Long.parseLong(data[1]));
					value.set(data[0]);
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		}

	}

}