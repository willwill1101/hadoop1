package com.ehl.dataselect.hbase.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期转换类
 * 
 * @author SDU.lwlin
 * 
 */
public class DateFormatter
{
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

	/**
	 * 将字符串转换为毫秒数，如果出错返回-1
	 * 
	 * @param str 字符串
	 * @return 
	 */
	public static synchronized long parse(String str)
	{
		return parse(str, -1);
	}

	/**
	 * 将字符串转换为毫秒数，如果出错返回指定值t_def
	 * @param str   字符串
	 * @param t_def 默认返回值
	 * @return 
	 */
	public static synchronized long parse(String str, long t_def)
	{
		try
		{
			long ts = format.parse(str).getTime();
			return ts;
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		return t_def;
	}

	/**
	 * 
	 * 格式化毫秒数为 yyyy-MM-dd HH:mm:ss SSS
	 * 
	 * @param t
	 *              the timestamp
	 * @return the natural format string value
	 */
	public static synchronized String format(long t)
	{
		return format.format(new Date(t));
	}

	public static void main(String[] args)
	{
		// System.out.println(DateFormatter.format(-1));
	}
}
