package com.Butter.WordUnionFrequence;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordUnionFrequenceMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static List<String> wordsList = null;
	private static Text text = new Text();
	private final static IntWritable one = new IntWritable(1);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		// 读取分发的词频文件
		if (wordsList == null) {
			wordsList = new LinkedList<String>();
			File file = new File("frequence");
			if (file.exists()) {
				DataInputStream in = new DataInputStream(new FileInputStream(
						file));
				String str = null;
				try {
					while ((str = in.readUTF()) != null) {
						wordsList.add(str);
					}
				} catch (EOFException e) {
					in.close();
				}
				return;
			} else {
				System.err.println("Read frequence error");
				System.exit(2);
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 取出每一条微博分词后的结果
		String[] temp = value.toString().split("<split>");
		if (temp.length == 8) {
			String[] data = temp[1].split(" ");
			HashSet<String> dataUnion = new HashSet<String>();
			// 去除重复数据
			for (String s : data) {
				int index = s.indexOf("/");
				if (index != -1) {
					String sTemp = s.substring(0, index);
					if (!dataUnion.contains(sTemp))
						dataUnion.add(sTemp);
				}
			}
			// 计算下标
			int[] wordsIndexOf = new int[wordsList.size()];
			int flag = 0;
			for (String s : dataUnion) {
				int indexOf = wordsList.indexOf(s);
				if (indexOf > -1)
					wordsIndexOf[flag++] = indexOf;
			}
			// 对下标排序
			Arrays.sort(wordsIndexOf, 0, flag);
			// 对于每一个词语p1和p2，输出<"p1,p2",1>
			for (int i = 0; i < flag; ++i)
				for (int j = i + 1; j < flag; ++j) {
					text.set(wordsIndexOf[i] + "," + wordsIndexOf[j]);
					context.write(text, one);
				}
		}
	}

}