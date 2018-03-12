package com.Butter.WordUnionFrequence;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class WordFrequenceFileUtil {

	public void distributedWordFrequenceFile(String pathDir, int maxWords,
			Configuration conf) throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		Path inPath = new Path(pathDir);
		inPath = inPath.makeQualified(inPath.getFileSystem(conf));
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
			wordList.add(words[1]);
			i++;
		}
		in.close();
		// 写文件
		Path fileOutPath = new Path(pathDir, "frequence");
		FSDataOutputStream out = hdfs.create(fileOutPath);
		for (String s : wordList) {
			out.writeUTF(s);
		}
		out.flush();
		out.close();
		// 分发词频文件
		DistributedCache.createSymlink(conf);
		URI tempURI = new URI(fileOutPath.toUri().toString() + "#frequence");
		DistributedCache.addCacheFile(tempURI, conf);
	}

}