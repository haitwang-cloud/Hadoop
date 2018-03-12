package com.Butter.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class WordUnionFrequence {

	List<String> wordList = new LinkedList<String>();

	public void statistics() {
		String[] words = { "a", "b", "c", "d" };
		for (String s : words)
			wordList.add(s);
		String[] data = "d<split>a<split>b<split>c".split("<split>");
		HashSet<String> dataUnion = new HashSet<String>();
		for (String s : data)
			if (!dataUnion.contains(s))
				dataUnion.add(s);
		int[] wordsIndexOf = new int[words.length];
		int flag = 0;
		for (String s : dataUnion) {
			int indexOf = wordList.indexOf(s);
			if (indexOf > -1)
				wordsIndexOf[flag++] = indexOf;
		}
		Arrays.sort(wordsIndexOf, 0, flag);
		for (int i = 0; i < flag; ++i)
			for (int j = i + 1; j < flag; ++j)
				System.out.println("[" + wordsIndexOf[i] + ","
						+ wordsIndexOf[j] + "]:" + words[wordsIndexOf[i]] + " " + words[wordsIndexOf[j]]);
	}

	public static void main(String[] args) {
		new WordUnionFrequence().statistics();
		StringBuilder builder = new StringBuilder();
		builder.append("abcded");
		builder.delete(0, builder.length()-1);
		System.out.println(builder.toString());
	}

}
