package com.Butter.Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;

import com.Butter.Cluster.APCluster;
import com.Butter.Cluster.ClusterInterface;
import com.Butter.WordSort.WordSortJob;
import com.Butter.WordSplit.WordSplitJob;
import com.Butter.WordUnionFrequence.WordUnionFrequenceJob;

public class Rumour {

	/**
	 * 程序入口
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// 判断输入参数是否合法
			if (args.length != 3) {
				System.err.println("Parameter error");
				System.exit(2);
			}
			// 第一个参数为reduce个数
			String reduceNums = args[0];
			// 第二个参数为输入路径
			String inputPath = args[1];
			// 第三个参数为输出结果(暂时未用)
			String outputPath = args[2];
			// 在分布式文件系统上的临时路径
			String wordSplitTemp = "/user/butter/word_split_temp";
			int res = 1;
			// 分词并统计词频
			res = ToolRunner.run(new JobConf(), new WordSplitJob(),
					new String[] { reduceNums, inputPath, wordSplitTemp });
			// 对词频排序
			if (res == 0)
				res = ToolRunner.run(new JobConf(), new WordSortJob(),
						new String[] { reduceNums,
								wordSplitTemp + "/wordFrequence",
								wordSplitTemp + "/wordFrequenceSort" });
			else
				System.exit(res);
			// 取前500个词频进行联合词频统计
			if (res == 0)
				res = ToolRunner.run(new JobConf(),
						new WordUnionFrequenceJob(), new String[] { reduceNums,
								wordSplitTemp + "/wordSplitData",
								wordSplitTemp + "/wordUnionFrequence",
								wordSplitTemp + "/wordFrequenceSort", "500" });
			else
				System.exit(res);
			// 仿射聚类(暂时串行)
			if (res == 0) {
				ClusterInterface cluster = new APCluster();
				cluster.doCluster(wordSplitTemp + "/wordFrequenceSort",
						wordSplitTemp + "/wordUnionFrequence", wordSplitTemp
								+ "/cluster", 227566, 500, 0.3,
						new Configuration());
			}
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
