package com.ehl.tvc.realtime.esbolt.util;

import java.util.UUID;

public class UUIDGenerator {
	
	public static String getUUID(){
		String s=UUID.randomUUID().toString();
		StringBuffer sb=new StringBuffer();
		sb.append(s.substring(0, 8)).append(s.substring(9, 13)).append(s.substring(14, 18)).append(s.substring(19, 23)).append(s.substring(24));
		return sb.toString();
	}
}