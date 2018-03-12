package com.Butter.Test;

import java.io.EOFException;
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

public class HDFSFileTest {

	public void readWordFrequenceFile(String pathDir, int maxWords)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path inPath = new Path(pathDir);
		inPath = inPath.makeQualified(inPath.getFileSystem(conf));
		// System.out.println(inPath.toString());
		FileStatus fileStatus = hdfs.getFileStatus(inPath);
		if (!fileStatus.isDir()) {
			System.err.print("该路径不是目录");
			System.exit(2);
		}
		Path wordFrequencePath = null;
		for (FileStatus fs : hdfs.listStatus(inPath)) {
			if (fs.isDir())
				continue;
			String fileName = fs.getPath().getName();
			if (fileName.matches("part.*")) {
				wordFrequencePath = fs.getPath();
				break;
			}
		}
		if (wordFrequencePath == null) {
			System.err.print("词频文件读取错误");
			System.exit(2);
		}
		FSDataInputStream in = hdfs.open(wordFrequencePath);
		LineReader lineReader = new LineReader(in, conf);
		int i = 0;
		Text msg = new Text();
		List<String> wordList = new LinkedList<String>();
		while (lineReader.readLine(msg) > 0 && i < maxWords) {
			String[] words = msg.toString().split("	");
			words = words[1].split("/");
			wordList.add(words[0]);
			i++;
		}
		in.close();
		// 写文件
		Path fileOutPath = new Path(pathDir, "frequence");
		FSDataOutputStream out = hdfs.create(fileOutPath);
		for (String s : wordList) {
			out.writeUTF(s);
			// System.out.println(s);
		}
		out.flush();
		out.close();
		// 测试读
		in = hdfs.open(fileOutPath);
		String str = null;
		try {
			while ((str = in.readUTF()) != null) {
				System.out.println(str);
			}
		} catch (EOFException e) {
			in.close();
		}
	}

	public static void main(String[] args) throws IOException {
		new HDFSFileTest().readWordFrequenceFile("/user/butter/output/", 200);
	}

}
