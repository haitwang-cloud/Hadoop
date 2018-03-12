package com.Butter.Cluster;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class WordCluster {

	private int[] wordFrequenceArray = null;
	private String[] wordArray = null;
	private int[][] unionWordsFrequence = null;
	private List<List<Integer>> cluster = new LinkedList<>();

	/**
	 * 
	 * @param frequenceSortDir
	 * @param unionFrequenceDir
	 * @param conf
	 * @throws IOException
	 *             frequenceSortDir 词频文件夹路径 unionFrequenceDir 为联合词频文件夹路径
	 */
	public void init(String frequenceSortDir, String unionFrequenceDir,
			int wordMax, Configuration conf) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		// 读取词频文件
		Path inPath = new Path(frequenceSortDir);
		inPath = inPath.makeQualified(inPath.getFileSystem(conf));
		FileStatus fileStatus = hdfs.getFileStatus(inPath);
		if (!fileStatus.isDir()) {
			System.err.print("该路径不是目录");
			System.exit(2);
		}
		Text msg = new Text();
		for (FileStatus fs : hdfs.listStatus(inPath)) {
			if (fs.isDir())
				continue;
			String fileName = fs.getPath().getName();
			if (fileName.matches("part.*")) {
				wordFrequenceArray = new int[wordMax];
				wordArray = new String[wordMax];
				FSDataInputStream in = hdfs.open(fs.getPath());
				LineReader lineReader = new LineReader(in, conf);
				int i = 0;
				while (lineReader.readLine(msg) > 0 && (i < wordMax)) {
					String[] temp = msg.toString().split("	");
					wordFrequenceArray[i] = Integer.parseInt(temp[0]);
					wordArray[i] = temp[1];
					i++;
				}
				in.close();
				break;
			}
		}
		// 读取联合词频文件，并存放到数组中
		unionWordsFrequence = new int[wordMax][wordMax];
		inPath = new Path(unionFrequenceDir);
		inPath = inPath.makeQualified(inPath.getFileSystem(conf));
		fileStatus = hdfs.getFileStatus(inPath);
		if (!fileStatus.isDir()) {
			System.err.print("该路径不是目录");
			System.exit(2);
		}
		Path wordUnionFrequenceFile = null;
		for (FileStatus fs : hdfs.listStatus(inPath)) {
			if (fs.isDir())
				continue;
			String fileName = fs.getPath().getName();
			if (fileName.matches("part.*")) {
				wordUnionFrequenceFile = fs.getPath();
				FSDataInputStream unionIn = hdfs.open(wordUnionFrequenceFile);
				LineReader lineReader = new LineReader(unionIn, conf);
				while (lineReader.readLine(msg) > 0) {
					// 1,1 200
					String[] words = msg.toString().split("	");
					String[] temp = words[0].split(",");
					// 解析
					int x = Integer.parseInt(temp[0]);
					int y = Integer.parseInt(temp[1]);
					int num = Integer.parseInt(words[1]);
					// 将都取到的联合词频存放到矩阵中
					unionWordsFrequence[x][y] = unionWordsFrequence[y][x] = num;
				}
				unionIn.close();
			}
		}
	}

	// 增量聚类最大值
	public void clusterMax(double min) {
		// 初始化
		List<Integer> temp = new LinkedList<>();
		temp.add(0);
		cluster.add(temp);
		for (int index = 1; index < wordFrequenceArray.length; ++index) {
			// 设置距离最小数值
			double minDistance = Double.MAX_VALUE;
			int flag = -1;
			for (int i = 0; i < cluster.size(); ++i) {
				List<Integer> eachCluster = cluster.get(i);
				double averageInEachCluster = Double.MIN_VALUE - 1;
				// 对每一簇计算条件概率,求出条件概率最大值
				for (Integer t : eachCluster) {
					double relavent = ((double) unionWordsFrequence[index][t])
							/ ((double) wordFrequenceArray[index]);
					if (relavent > averageInEachCluster)
						averageInEachCluster = relavent;
					// averageInEachCluster += relavent;
				}
				// averageInEachCluster /= (double) eachCluster.size();
				if (averageInEachCluster > 0) {
					double distance = 1 / averageInEachCluster;
					if (distance < minDistance) {
						minDistance = distance;
						flag = i;
					}
				}
			}
			if (minDistance <= min) {
				List<Integer> eachCluster = cluster.get(flag);
				eachCluster.add(index);
			} else {
				List<Integer> eachCluster = new LinkedList<>();
				eachCluster.add(index);
				cluster.add(eachCluster);
			}
		}
	}

	// 增量聚类平均
	public void clusterAverage(double min) {
		// 初始化
		List<Integer> temp = new LinkedList<>();
		temp.add(0);
		cluster.add(temp);
		for (int index = 1; index < wordFrequenceArray.length; ++index) {
			// 设置距离最小数值
			double minDistance = Double.MAX_VALUE;
			int flag = -1;
			for (int i = 0; i < cluster.size(); ++i) {
				List<Integer> eachCluster = cluster.get(i);
				double averageInEachCluster = 0;
				// 对每一簇计算条件概率,求出条件概率最大值
				for (Integer t : eachCluster) {
					double relavent = ((double) unionWordsFrequence[index][t])
							/ ((double) wordFrequenceArray[index]);
					averageInEachCluster += relavent;
				}
				averageInEachCluster /= (double) eachCluster.size();
				if (averageInEachCluster > 0) {
					double distance = 1 / averageInEachCluster;
					if (distance < minDistance) {
						minDistance = distance;
						flag = i;
					}
				}
			}
			if (minDistance <= min) {
				List<Integer> eachCluster = cluster.get(flag);
				eachCluster.add(index);
			} else {
				List<Integer> eachCluster = new LinkedList<>();
				eachCluster.add(index);
				cluster.add(eachCluster);
			}
		}
	}
	
	public void saveResultToHDFS(String outputDir, Configuration conf)
			throws IOException {
		// 保存聚类结果
		FileSystem hdfs = FileSystem.get(conf);
		Path outPath = new Path(outputDir);
		outPath = outPath.makeQualified(outPath.getFileSystem(conf));
		outPath = new Path(outPath, "cluster");
		FSDataOutputStream out = hdfs.create(outPath);
		for (List<Integer> eachCluster : cluster) {
			StringBuilder builder = new StringBuilder();
			for (Integer e : eachCluster) {
				builder.append(wordArray[e] + "|" + wordFrequenceArray[e] + " ");
			}
			out.write((builder.toString().trim() + "\n").getBytes("UTF-8"));
		}
		out.flush();
		out.close();
	}

	public void showTest() {

		/*
		 * for (int i = 0; i < wordFrequenceArray.length; ++i) {
		 * System.out.println(wordFrequenceArray[i] + " " + wordArray[i]); }
		 */
		/*
		 * for (int i = 0; i < 10; ++i) { for (int j = 0; j <
		 * unionWordsFrequence.length; ++j)
		 * System.out.print(unionWordsFrequence[i][j] + " ");
		 * System.out.println(); }
		 */

		for (List<Integer> eachCluster : cluster) {
			for (Integer e : eachCluster) {
				System.out.print(wordArray[e] + "|" + wordFrequenceArray[e]
						+ " ");
			}
			System.out.println();
		}
	}

	// 聚类
	public void doCluster(String frequenceSortDir, String unionFrequenceDir,
			String outputDir, int wordMax, double limit, Configuration conf)
			throws IOException {
		init("/user/butter/word_split_temp/wordFrequenceSort",
				"/user/butter/word_split_temp/wordUnionFrequence", wordMax, conf);
		clusterMax(limit);
		saveResultToHDFS(outputDir, conf);
	}

	public static void main(String[] args) throws IOException {
		WordCluster wordCluster = new WordCluster();
		wordCluster.init("/user/butter/word_split_temp/wordFrequenceSort",
				"/user/butter/word_split_temp/wordUnionFrequence", 500,
				new Configuration());
		wordCluster.clusterMax(5);
		wordCluster.saveResultToHDFS("/user/butter/word_split_temp",
				new Configuration());
		wordCluster.showTest();
	}

}