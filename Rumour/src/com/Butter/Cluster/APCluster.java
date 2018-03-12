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

/*
 * 仿射传播聚类
 *
 */
public class APCluster implements ClusterInterface {

	private String[] words = null;
	private int[] frequence = null;
	private int[][] unionFrequence = null;
	private double s[][] = null;
	private double a[][] = null;
	private double r[][] = null;

	// 聚类结果
	private List<List<Integer>> wordCluster = null;

	/**
	 * 初始化矩阵
	 * @param frequenceSortDir
	 *     词频文件路径
	 * @param unionFrequenceDir
	 *     联合词频文件路径
	 * @param wordMax
	 *     需要聚类的词语个数
	 * @param conf
	 * @throws IOException
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
				frequence = new int[wordMax];
				words = new String[wordMax];
				FSDataInputStream in = hdfs.open(fs.getPath());
				LineReader lineReader = new LineReader(in, conf);
				int i = 0;
				while (lineReader.readLine(msg) > 0 && (i < wordMax)) {
					String[] temp = msg.toString().split("	");
					frequence[i] = Integer.parseInt(temp[0]);
					words[i] = temp[1];
					i++;
				}
				in.close();
				break;
			}
		}
		// 读取联合词频文件，并存放到数组中
		unionFrequence = new int[wordMax][wordMax];
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
					unionFrequence[x][y] = unionFrequence[y][x] = num;
				}
				unionIn.close();
			}
		}
		s = new double[wordMax][wordMax];
		a = new double[wordMax][wordMax];
		r = new double[wordMax][wordMax];
	}

	/**
	 * 两个词语之间的相似度计算
	 * 基于条件概率方法
	 * @param limit
	 *     阈值
	 */
	public void createMS(double limit) {
		double avg = 0;
		for (int i = 0; i < unionFrequence.length; ++i)
			for (int j = 0; j < unionFrequence.length; ++j) {
				double temp = (double) unionFrequence[i][j]
						/ (double) frequence[i];
				s[i][j] = temp;
				avg += temp;
			}
		avg /= (unionFrequence.length * unionFrequence.length);
		System.out.println(avg);
		for (int i = 0; i < unionFrequence.length; ++i)
			s[i][i] = limit;
	}

	/**
	 * 两个词语之间的相似度计算
	 * 基于杠杆率的方法
	 * @param sampleNum
	 *     样本总数
	 * @param limit
	 *     阈值
	 */
	public void createMS(int sampleNum, double limit) {
		for (int i = 0; i < unionFrequence.length; ++i)
			for (int j = i + 1; j < unionFrequence.length; ++j) {
				double p_i_j = (double) unionFrequence[i][j]
						/ (double) sampleNum;
				double p_i = (double) frequence[i] / (double) sampleNum;
				double p_j = (double) frequence[j] / (double) sampleNum;
				double max_i_j = 0;
				if (p_i > p_j)
					max_i_j = p_j - p_i * p_j;
				else
					max_i_j = p_i - p_i * p_j;
				double sim = (p_i_j - p_i * p_j) / max_i_j;
				if (sim < 0)
					sim = 0;
				s[i][j] = s[j][i] = sim;
			}
		for (int i = 0; i < unionFrequence.length; ++i)
			s[i][i] = limit;
	}

	/**
	 * 放射传播聚类
	 */
	public void calculate() {
		// 搜索聚类中心
		for (int t = 0; t < 1; t++) {
			for (int i = 0; i < unionFrequence.length; ++i) {
				for (int k = 0; k < unionFrequence.length; ++k) {
					double max = Double.MIN_VALUE - 1;
					for (int kp = 0; kp < unionFrequence.length; kp++) {
						if (kp != k)
							max = Math.max(max, a[i][kp] + s[i][kp]);
					}
					r[i][k] = s[i][k] - max;
				}
			}
			for (int k = 0; k < unionFrequence.length; ++k) {
				for (int i = 0; i < unionFrequence.length; ++i) {
					double sum = 0;
					for (int ip = 0; ip < unionFrequence.length; ++ip) {
						if (ip != i && ip != k) {
							sum += Math.max(0, r[ip][k]);
						}
					}
					if (i != k) {
						a[i][k] = Math.min(0, r[k][k] + sum);
					} else {
						a[k][k] = sum;
					}
				}
			}
		}
		List<Integer> list = new LinkedList<Integer>();
		wordCluster = new LinkedList<>();
		// 判断聚类中心
		for (int k = 0; k < unionFrequence.length; ++k) {
			if (a[k][k] + r[k][k] > 0) {
				list.add(k);
				List<Integer> temp = new LinkedList<Integer>();
				temp.add(k);
				wordCluster.add(temp);
			}
		}
		// 完成聚类
		for (int i = 0; i < frequence.length; ++i) {
			if (list.indexOf(i) < 0) {
				int t = 0;
				int flag = 0;
				double temp = 0;
				double max = a[i][t] + r[i][t];
				for (int index = 1; index < list.size(); ++index) {
					t = list.get(index);
					temp = (a[i][t] + r[i][t]);
					if (max < temp) {
						max = temp;
						flag = index;
					}
				}
				List<Integer> cluster = wordCluster.get(flag);
				cluster.add(i);
			}
		}
		/*
		 * // 显示测试 for (List<Integer> cluster : wordCluster) { for (int i :
		 * cluster) System.out.print(words[i] + " "); System.out.println(); }
		 */
	}

	/**
	 * 保存聚类结果
	 * @param outputDir
	 *     保存路径
	 * @param conf
	 * @throws IOException
	 */
	public void saveResultToHDFS(String outputDir, Configuration conf)
			throws IOException {
		// 保存聚类结果
		FileSystem hdfs = FileSystem.get(conf);
		Path outPath = new Path(outputDir);
		outPath = outPath.makeQualified(outPath.getFileSystem(conf));
		outPath = new Path(outPath, "cluster");
		FSDataOutputStream out = hdfs.create(outPath);
		for (List<Integer> eachCluster : wordCluster) {
			if (eachCluster.size() > 1) {
				StringBuilder builder = new StringBuilder();
				for (Integer e : eachCluster) {
					builder.append(words[e]).append(" ");
				}
				out.write((builder.toString().trim() + "\n").getBytes("UTF-8"));
			}
		}
		out.flush();
		out.close();
	}

	// 测试用
	public static void main(String[] args) throws IOException {
		APCluster test = new APCluster();
		test.init("/user/butter/word_split_temp/wordFrequenceSort",
				"/user/butter/word_split_temp/wordUnionFrequence", 500,
				new Configuration());
		test.createMS(227566, 0.3);
		test.calculate();
	}

	/**
	 * 仿射传播聚类
	 */
	@Override
	public void doCluster(String frequenceSortDir, String unionFrequenceDir,
			String outputDir, int simpleNum, int wordMax, double limit,
			Configuration conf) throws IOException {
		// 初始化矩阵
		init(frequenceSortDir, unionFrequenceDir, wordMax, conf);
		// 计算词语之间相似度
		createMS(simpleNum, limit);
		// 仿射传播聚类
		calculate();
		// 保存聚类结果
		saveResultToHDFS(outputDir, conf);
	}

}