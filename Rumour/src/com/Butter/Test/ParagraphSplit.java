package com.Butter.Test;

import java.io.UnsupportedEncodingException;

import ICTCLAS.I3S.AC.ICTCLAS50;

public class ParagraphSplit {

	private ICTCLAS50 testICTCLAS50 = new ICTCLAS50();

	public ParagraphSplit() throws Exception {
		System.loadLibrary("ICTCLAS50");
		String argu = ".";
		// 初始化
		if (testICTCLAS50.ICTCLAS_Init(argu.getBytes("GB2312")) == false) {
			System.out.println("Init Fail!");
			System.exit(0);
		}
		testICTCLAS50.ICTCLAS_SetPOSmap(0);
		int nCount = 0;
		String usrdir = "userdict.txt"; // 用户词典文件
		byte[] usrdirb = usrdir.getBytes();
		nCount = testICTCLAS50.ICTCLAS_ImportUserDictFile(usrdirb, 0);
		System.out.println("导入用户词典个数:" + nCount);
	}

	public String wordSplit(String sInput) throws UnsupportedEncodingException {
		// 去除@
		sInput = sInput.replaceAll("@[\\u4e00-\\u9fa5\\w\\-]+", "");
		// 去除#
		sInput = sInput.replaceAll("#([^\\#|.]+)#", "");
		// 去除/和:
		sInput = sInput.replaceAll("[/:]", "");
		// 初始化
		byte nativeBytes[] = testICTCLAS50.ICTCLAS_ParagraphProcess(
				sInput.getBytes("GB2312"), 2, 1);
		String nativeStr = new String(nativeBytes, 0, nativeBytes.length,
				"GB2312");
		String[] result = nativeStr.split(" ");
		for (String s : result) {
			if (s.equals("") || s.equals(" "))
				continue;
			if (s.matches(".*/n.*|.*/m.*|.*/s.*|.*/an.*|.*/v[dnxig]*|.*/b.*"))
				System.out.println(s);
		}
		return nativeStr;
	}
	
	public void destory() {
		testICTCLAS50.ICTCLAS_SaveTheUsrDic();
		testICTCLAS50.ICTCLAS_Exit();
	}

	public static void main(String[] args) {
		ParagraphSplit split = null;
		try {
			split = new ParagraphSplit();
			System.out.println(split.wordSplit("@小艳子kiki @光影魔术师之择日而栖 @就是爱黑巧克力 尝试新的外景风格，亲们，我有木有拍婚纱照的潜质？"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (split != null)
				split.destory();
		}
	}
	
}