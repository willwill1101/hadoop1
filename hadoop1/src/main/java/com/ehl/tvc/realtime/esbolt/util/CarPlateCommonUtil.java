package com.ehl.tvc.realtime.esbolt.util;

import com.ehl.tvc.realtime.esbolt.dao.DictSynchronizeService;

public class CarPlateCommonUtil {
	
	public static final String SPERATOR_STR = "-";

	/**
	 * @param carPlateWithoutPreffix 不包含省份、wj等前缀的车牌号
	 * @return es 车牌号索引
	 */
	public static String produceCarPlateIndexStrWithoutPre(String carPlateWithoutPreffix) {
		int plateSize = carPlateWithoutPreffix.length();
		StringBuilder indexStringBuilder = new StringBuilder();
		for(int indexSize=plateSize-1;indexSize>=2;indexSize--) {
			String indexStr = getFixedSizeIndexStr(carPlateWithoutPreffix,indexSize);
			if(indexStr!=null) {
				indexStringBuilder.append(indexStr);
			}
		}
		if(indexStringBuilder.length()<=0) {
			return null;
		}
		//删除首个连字符
		return carPlateWithoutPreffix+indexStringBuilder;
	}
	
	/**
	 * @param carPlate 完整车牌号
	 * @return es 车牌号索引
	 */
	public static String produceCarPlateIndexStr(String carPlate) {
		String carPlateWithoutPre = getCarPlateWithoutPrefix(carPlate);
		return produceCarPlateIndexStrWithoutPre(carPlateWithoutPre);
	}

	private static String getFixedSizeIndexStr(String carPlateWithoutPreffix, int indexSize) {
		StringBuilder sb = new StringBuilder();
		if(indexSize>carPlateWithoutPreffix.length()) {
			return null;
		}
		for(int i=0;i<=carPlateWithoutPreffix.length()-indexSize;i++) {
			sb.append(SPERATOR_STR).append(carPlateWithoutPreffix.substring(i,i+indexSize));
		}
		return sb.toString();
	}
	
	public static String getPrefixFromCarPlate(String carPlate) {
		if(carPlate.length()<2) throw new IllegalArgumentException("车牌格式错误，车牌号为:"+carPlate);
		//判断前位是否为前缀
		String carPre = carPlate.substring(0, 1);
		boolean isPre =DictSynchronizeService.getInstance().isPreFix(carPre);
		if(!isPre){//不是前缀判断两位
			 carPre = carPlate.substring(0, 1);
			 isPre =DictSynchronizeService.getInstance().isPreFix(carPre);
			 if(!isPre){
				 throw new IllegalArgumentException("车牌格式错误，车牌号为:"+carPlate);
			 }
		}
			return carPre;
		
	}
	
	public static String getCarPlateWithoutPrefix(String carPlate) {
		return carPlate.substring(getPrefixFromCarPlate(carPlate).length());
	}
	
	public static void main(String[] args) {
//		final String testCarPlate = "MA1726";
//		System.out.println(produceCarPlateIndexStr(testCarPlate));
		
		System.out.println(produceCarPlateIndexStrWithoutPre("HLD006"));
//		final String testCarPlate1 = "MA172";
//		System.out.println(produceCarPlateIndexStr(testCarPlate1));
	}
}
