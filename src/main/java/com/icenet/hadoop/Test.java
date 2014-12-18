package com.icenet.hadoop;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Test {

	public static void main(String[] args) {

		String str = "hello world hadoop itcast";
		
		/*String[] ss = str.split(" ");
		
		for(String s:ss){
			System.out.println(s);
		}*/
		
		
		String word = new String();//Text word = new Text();
		Map<String, Integer> map = new HashMap<String, Integer>();
		StringTokenizer st = new StringTokenizer(str);
		
		while(st.hasMoreTokens()){
			
			String s1 = st.nextToken();
			word = s1;//word.set(s1);
			map.put(word, 1);
			System.out.println(s1);
		}
		
	}

}
