package com.Butter.ICTCLAS;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import ICTCLAS.I3S.AC.ICTCLAS50;

public class ParagraphSplit implements StopWord{

	private ICTCLAS50 testICTCLAS50 = null;

	private HashSet<String> stopTable;

	private static ParagraphSplit paragraphSplit = null;

	public synchronized static ParagraphSplit getInstance() {
		if (paragraphSplit == null)
			try {
				paragraphSplit = new ParagraphSplit();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return paragraphSplit;
	}

	private ParagraphSplit() throws Exception {
		// 调用分词工具提供的JNI的接口
		System.loadLibrary("ICTCLAS50");
		// 将停词读入到HashSet中
		stopTable = new HashSet<String>();
		for (int i = 0; i < stopWords.length; ++i) {
			stopTable.add(stopWords[i]);
		}
		testICTCLAS50 = new ICTCLAS50();
		String argu = ".";
		// 初始化
		if (testICTCLAS50.ICTCLAS_Init(argu.getBytes("GB2312")) == false) {
			System.out.println("Init Fail!");
			System.exit(0);
		}
		// 设置词性标注集(0 计算所二级标注集，1 计算所一级标注集，2 北大二级标注集，3 北大一级标注集)
		testICTCLAS50.ICTCLAS_SetPOSmap(0);
		// 导入用户字典
		int nCount = 0;
		String usrdir = "userdict.txt"; // 用户字典路径
		byte[] usrdirb = usrdir.getBytes();// 将string转化为byte类型
		// 导入用户字典,返回导入用户词语个数第一个参数为用户字典路径，第二个参数为用户字典的编码类型
		nCount = testICTCLAS50.ICTCLAS_ImportUserDictFile(usrdirb, 0);
		// System.out.println("导入用户词个数" + nCount);
	}

	public List<String> wordSplit(String sInput)
			throws UnsupportedEncodingException {
		// 去除@
		sInput = sInput.replaceAll("@[\\u4e00-\\u9fa5\\w\\-]+", "");
		// 去除#
		sInput = sInput.replaceAll("#([^\\#|.]+)#", "");
		// 去除/
		sInput = sInput.replaceAll("[/:]", "");
		// 导入用户字典后再分词
		byte nativeBytes[] = testICTCLAS50.ICTCLAS_ParagraphProcess(
				sInput.getBytes("GB2312"), 2, 1);
		String nativeStr = new String(nativeBytes, 0, nativeBytes.length,
				"GB2312");
		List<String> result = new LinkedList<String>();
		String[] temp = nativeStr.split(" ");
		for (String s : temp) {
			if (s.equals("") || s.equals(" "))
				continue;
			if (s.matches(".*/n.*|.*/m.*|.*/s.*|.*/an.*|.*/v[dnxig]*")) {
				String words = s.substring(0, s.indexOf("/")).trim();
				if (!stopTable.contains(words))
					result.add(s.trim());
			}
		}
		return result;
	}

	public void destory() {
		// 保存用户字典
		testICTCLAS50.ICTCLAS_SaveTheUsrDic();
		// 释放分词组件资源
		testICTCLAS50.ICTCLAS_Exit();
	}

}