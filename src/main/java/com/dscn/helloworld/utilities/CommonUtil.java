package com.dscn.helloworld.utilities;

import java.util.*;

public class CommonUtil {
	public static String joinHostPort(String hostList, String port) {
		String result = "";
		
		String[] hostArr = hostList.split(",");
		for (int i = 0; i < hostArr.length; i++) {
			result += hostArr[i] + ":" + port;
			if (i != (hostArr.length - 1)) {
				result += ",";
			}
		}

		return result;
	}

	public static List<String> strToList(String str) {
		List<String> result = new ArrayList<String>();
		String[] strArr = str.split(",");

		for (int i = 0; i < strArr.length; i++) {
			result.add(strArr[i]);
		}

		return result;
	}
}