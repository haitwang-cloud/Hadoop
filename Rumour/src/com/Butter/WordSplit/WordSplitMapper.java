package com.Butter.WordSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.Butter.ICTCLAS.ParagraphSplit;

public class WordSplitMapper extends
		Mapper<LongWritable, Text, Text, DataValue> {

	private ParagraphSplit paragraphSplit = null;

	private final static IntWritable one = new IntWritable(1);

	private static DataValue data = new DataValue();

	private final static Text splitTextData = new Text("<splitTextData>");

	private static Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		try {
			// 创建分词工具类
			paragraphSplit = ParagraphSplit.getInstance();
			// 读入一行数据，每一行数据代表一条微博，每一条微博格式如下:文章编号<split>正文<split>评论数<split>插入时间<split>来源<split>人物所属ID<split>正文发布时间<split>转发次数
			String[] temp = value.toString().split("<split>");
			List<String> result = null;
			StringBuilder strBuilder = new StringBuilder();
			if (temp.length == 8) {
				// 读取正文并调用分词工具分词
				result = paragraphSplit.wordSplit(temp[1]);
				// 首先统计每一条微博中的词语个数
				Map<String, Integer> map = new HashMap<String, Integer>();
				for (String s : result) {
					// 分词结果会带有每个词的词性,比如:打/d，这里需要去除/d
					String sTemp = s.substring(0, s.indexOf("/"));
					Integer num = null;
					if ((num = map.get(sTemp)) == null) {
						num = new Integer(1);
					} else {
						num++;
					}
					map.put(sTemp, num);
					strBuilder.append(s + " ");
				}
				Set<String> keySet = map.keySet();
				// 对于每一个词语就输出<词语,1>
				for (String s : keySet) {
					word.set(s);
					data.set(one);
					// 统计词频
					context.write(word, data);
				}
				// 对分词结果做进一步处理
				temp[1] = strBuilder.toString().trim();
				strBuilder.delete(0, strBuilder.length());
				for (int i = 0; i < temp.length; ++i) {
					String s = temp[i];
					if (i != 7)
						strBuilder.append(s + "<split>");
					else
						strBuilder.append(s);
				}
				word.set(strBuilder.toString().trim());
				data.set(word);
				// 对于每条微博就输出<"splitTextData",微博内容>
				context.write(splitTextData, data);
			} else
				System.out.println(value.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}