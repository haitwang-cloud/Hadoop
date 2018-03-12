package com.Butter.Cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface ClusterInterface {

	// 聚类
	public void doCluster(String frequenceSortDir, String unionFrequenceDir,
			String outputDir, int simpleNum, int wordMax, double limit, Configuration conf) throws IOException;
	
}
