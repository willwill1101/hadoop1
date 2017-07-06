package com.ehl.dataselect.business.server;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ehl.dataselect.hbase.persistent.TRFieldEnum;
import com.ehl.dataselect.hbase.persistent.TravelRecord;
import com.ehl.dataselect.util.DateFormatter;
import com.ehl.dataselect.util.MD5Coder;
import com.ehl.itgs.interfaces.bean.ResultCar;
import com.ehl.itgs.interfaces.bean.analysis.LocusCarResult;
import com.ehl.itgs.interfaces.bean.analysis.PassTgsInfo;
import com.ehl.rpc.imgserver.ImageServerManger;
import com.ehl.tvc.realtime.common.MD5Util;
import com.ehl.tvc.realtime.esbolt.bean.TrPlateEsBean;

/**
 * 
 * 
 */
public class TR2Result
{
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
	// web root

	//private final static String imgerroradd = imgroot + "/0/0/-1/-1";

	public static String getTimestamp(TravelRecord tr)
	{
		long ts = tr.getLongValue(TRFieldEnum.TIMESTAMP);
		String res = DateFormatter.format(ts);
		return res;
	}

	public static String getPlatenumber(TravelRecord tr)
	{
		return tr.getStringValue(TRFieldEnum.CAR_PLATE_NUMBER);
	}

	public static String getPlatetype(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAR_PLATE_TYPE));
	}

	public static short getSpeed(TravelRecord tr)
	{
		return (short) tr.getIntValue(TRFieldEnum.SPEED);
	}

	public static String getPlateColor(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAR_PLATE_COLOR));
	}

	public static String getCarColor(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAR_COLOR));
	}

	public static String getCarBrand(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAR_BRAND));
	}

	public static String getLandIe(TravelRecord tr)
	{
		return Integer.toString(tr.getIntValue(TRFieldEnum.LANE_ID));
	}

	public static String getCameraLocationId(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAMERA_LOCATION));
	}

	public static String getCameraOrientation(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAMERA_ORIENTATION));
	}

	public static int getTravelOrientation(TravelRecord tr)
	{
		return tr.getIntValue(TRFieldEnum.TRAVEL_ORIENTATION);
	}

	public static String getTgsId(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.BAY_ID));
	}

	public static String[] getUrls(TravelRecord tr)
	{
		String[] res = new String[3];
		for (int i = 0; i < res.length; i++)
			res[i] = tr.getStringValueOfStrings(TRFieldEnum.IMAGE_URLS, i);
		return res;
	}

	public static String getStatus(TravelRecord tr)
	{
		return String.valueOf(tr.getIntValue(TRFieldEnum.CAR_STATUS));
	}
	
	
	public static String getPlate_coordinates(TravelRecord tr)
	{
		int[] ints = tr.getIntArrayValue(TRFieldEnum.PLATE_COORDINATES);
		String str = null;
		for(int in:ints){
			if(str==null){
				str=""+in;
			}else{
				str+=","+in;
			}
		}
		return str;
		
	}
	
	
	public static String getDriver_coordinates(TravelRecord tr)
	{
		int[] ints = tr.getIntArrayValue(TRFieldEnum.DRIVER_COORDINATES);
		String str = null;
		for(int in:ints){
			if(str==null){
				str=""+in;
			}else{
				str+=","+in;
			}
		}
		return str;
	}

	public static TrPlateEsBean getResultCar(TravelRecord tr, boolean stream, boolean isMainCar) throws ParseException
	{
		
		TrPlateEsBean esBean = new TrPlateEsBean();
		esBean.setVersion("3");
		esBean.setTimestamp(format.parse(getTimestamp(tr)).getTime());
		esBean.setCar_plate_number(getPlatenumber(tr).toLowerCase());
		esBean.setSpeed(getSpeed(tr));
		esBean.setLane_id(getLandIe(tr));
		esBean.setCamera_location(getCameraLocationId(tr));
		esBean.setBay_id(getTgsId(tr));
		esBean.setCamera_orientation(getCameraOrientation(tr));
		esBean.setCar_brand(getCarBrand(tr));
		esBean.setCar_color(getCarColor(tr));
		esBean.setCar_plate_color(getPlateColor(tr));
		esBean.setCar_plate_type(getPlatetype(tr));
		esBean.setCar_status(getStatus(tr));
		esBean.setTravel_orientation(""+getTravelOrientation(tr));
		esBean.setPlate_coordinates(getPlate_coordinates(tr));
		esBean.setDriver_coordinates(getDriver_coordinates(tr));
		esBean.setId(MD5Util.MD5(getPlatenumber(tr)+"-"+getPlatetype(tr)+"-"+getTgsId(tr)+"-"+getTimestamp(tr)+"-"+""+getTravelOrientation(tr)+"-"+getLandIe(tr)));
		String[] realUrls = null;
		String[] url = getUrls(tr);
		if(checkUrl(url)){
			realUrls = getRealImg(url);
		}else{
			realUrls = getNewImg(url);
		}
		esBean.setTp1(realUrls.length>1?realUrls[0]:"");
		esBean.setTp2(realUrls.length>2?realUrls[1]:"");
		esBean.setTp3(realUrls.length>3?realUrls[2]:"");
		return esBean;
	}

	private static String[] getRealImg(String[] urls) {
		String imgroot = ImageServerManger.getPath();
		String[] res = new String[3];
		String imgurl1 = urls[0];
		String urltmp = imgurl1 + "/" + urls[1];
		if (urls[2] != null && urls[2].length() != 0){
			urltmp = "n/" + urls[2] + "/" + urltmp;
		}
		urltmp += "/" + MD5Coder.getCode(urltmp);
		res[0] = imgroot + "/" + urltmp;
		res[1] = "";
		res[2] = "";
		return res;
	}

	private static String[] getNewImg(String[] url) {
		// 如果图片本身带url直接返回
		if(url != null && url.length > 1){
			if(url[0].toLowerCase().startsWith("http://")){
				return url;
			}
		}
		// 拼接url
		String imgroot = ImageServerManger.getPath();
		String[] res = new String[url.length];
		if(url != null){
			for(int i=0; i < res.length; i++){
				if(url[i] != null){
					res[i] = imgroot + "/n/"+ url[i];
				}
			}
		}
		return res;
	}

	/**
	 * 老图片格式
	 * 	 0-1_1388547491070-16/1878 url[0]
	 *   0-1/222363864456 url[1]
	 *   2/20140101 url[2]
	 * @param url
	 * @return
	 */
	private static boolean checkUrl(String[] url) {
		if(url != null && url.length==3){
			if(url[2] != null && url[2].length() < 12){
				return true;
			}
		}
		return false;
		
	}

	// append this string to url, to be used in the situation when the image
	// in kafka has been rushed out
	private static String getStreamUrlEnd(TravelRecord tr)
	{
		String res = "";
		try
		{
			res = tr.getLongValue(TRFieldEnum.TIMESTAMP) + "/" + getTgsId(tr) + "/"
			          + URLEncoder.encode(getPlatenumber(tr), "UTF-8").replaceAll("%", "*");
		}
		catch (UnsupportedEncodingException e)
		{
			// e.printStackTrace();
		}
		return res;
	}

	public static String[] getRealImgOld(String[] urls, boolean stream, TravelRecord tr)
	{
		//获取图片地址
		String imgroot = ImageServerManger.getPath();
		String[] res = new String[3];
		String imgurl1 = urls[0];
		// stream and with image, from the stream consumer
		if (imgurl1 == null || imgurl1.length() == 0)
		{
			String urltmp = "n/0/0/" + urls[1] + "/" + getStreamUrlEnd(tr);
			urltmp += "/" + MD5Coder.getCode(urltmp);
			res[0] = imgroot + "/" + urltmp;
			res[1] = "";
			res[2] = "";
		}
		else if(imgurl1.toLowerCase().endsWith("jpg") ||imgurl1.toLowerCase().endsWith("jpeg")){
			res[0] = imgurl1;
			res[1] = "";
			res[2] = "";
		}else if(imgurl1.toLowerCase().startsWith("http")){
			res[0] = imgurl1;
			res[1] = "";
			res[2] = "";
		}
		else if (!imgurl1.toLowerCase().endsWith("jpg"))
		{
			// stream format, maybe called in some business
			if (stream)
			{
				String urltmp = "n/0/0/" + urls[1] + "/" + getStreamUrlEnd(tr);
				urltmp += "/" + MD5Coder.getCode(urltmp);
				res[0] = imgroot + "/" + urltmp;
				res[1] = "";
				res[2] = "";
			}
			else
			{
				int del = imgurl1.indexOf('/');
				// should have image but no image
				if (del < 0)
				{
					res[0] = imgroot + "/0/0/-1/-1";
					res[1] = "";
					res[2] = "";
				}
				// normal
				else
				{
					// old hdfs image
					String urltmp = imgurl1 + "/" + urls[1];
					// new hdfs image
					if (urls[2] != null && urls[2].length() != 0)
						urltmp = "n/" + urls[2] + "/" + urltmp;
					urltmp += "/" + MD5Coder.getCode(urltmp);
					res[0] = imgroot + "/" + urltmp;
					res[1] = "";
					res[2] = "";
				}
			}
		}
		// without image
		else
		{
			if (!imgurl1.startsWith("ftp"))
				imgurl1 = "http://" + imgurl1;
			res[0] = imgurl1;
			res[1] = "";
			res[2] = "";
		}

		return res;
	}

	


	public static PassTgsInfo getPassTgsInfo(TravelRecord tr)
	{
		PassTgsInfo passTgsInfo = new PassTgsInfo();
		String bidStr = getTgsId(tr);
		passTgsInfo.setTgsId(bidStr);
		String tsStr = getTimestamp(tr);
		passTgsInfo.setPassTime(tsStr);
		String[] imgUrls = getUrls(tr);
		String[] realUrls = getRealImgOld(imgUrls, false, tr);
		passTgsInfo.setImgUrl(realUrls);
		short speed = getSpeed(tr);
		passTgsInfo.setSpeed(Short.toString(speed));
		String trdir = Integer.toString(getTravelOrientation(tr));
		passTgsInfo.setTravelOrientation(trdir);
		return passTgsInfo;
	}

}
