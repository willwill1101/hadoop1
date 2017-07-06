package com.ehl.dataselect.hbase.fundamental.matchFunction.base;

import java.util.LinkedList;

import com.ehl.dataselect.hbase.persistent.TRFieldEnum;


/*
 * empty��null�ǲ�����
 * 
 * �ı���ʾ(String, String...)	���	�ڲ���ʾ(TRFieldEnum, Patter, Long, Integer)	��	ServerFilter
 * ������Match, Is, Between�����ı���������������ı������ۡ�
 * 
 */
/**
 * Modified at 2013.01.26 for LinePeaker. Notes by Lin.<br>
 * This class provides an interface for query, which is like SQL and enables programmers<br>
 * learn and use easily.
 * 转换规范查询条件为比较器识别的比较信息
 * @author SDU.lwlin
 */
public abstract class MatchFunction
{

	public static final String ERR_STR_ILL_ARGUMENT = "MatchFunction Parser: \"%s\".";
	public static final String ERR_STR_NOT_SUPPORTED = "MatchFunction Parser: field \"%s\" is not suppoted.";
	public static final String ERR_STR_UNKNOWN_FIELD = "MatchFunction Parser: field \"%s\" is unknown.";

	public static enum OP
	{
		AND, OR, MATCH, IS, BETWEEN
	}

	/**
	 * Telling whether a tuple matches this MatchFunction. 
	 * 
	 * @param bs the value of the tuple in form of byte array
	 * @return {@code true} if matches, {@code false} otherwise.
	 */
	public abstract boolean match(byte[] bs);

	private boolean firewalled = false;
	private boolean firewall = false;

	/**
	 * Checking whether this MatchFunction should be blocked by firewall.
	 * 未发现使用意义
	 * @return {@code true} if this MatchFunction could go through, {@code false} otherwise.
	 */
	public boolean firewall()
	{
		if (!firewalled)
		{
			try
			{
				//MFFirewall.mfFirewall(this);
				firewall = true;
			}
			catch (Exception e)
			{
				firewall = false;
			}
		}
		return firewall;
	}

	/**
	 * Parse a string to MatchFunction.
	 * 
	 * 
	 * @param str the string value
	 * @return the corresponding MatchFunction
	 */
	public static MatchFunction parse(final String str)
	{
		if (str == null)
			return null;
		return parseSingle(new PeekLineReader(str), 1);
	}

	/**
	 * Parse a single line to MatchFunction.
	 * 
	 * @param reader the content provider
	 * @param level the current level
	 * @return the corresponding MatchFunction
	 */
	private static MatchFunction parseSingle(final PeekLineReader reader, final int level)
	{
		final String line = reader.peek();
		// log.debug("in level: " + level + ", peekLine=\t" + line);
		if (line != null && line.trim().length() != line.length()) //无意义
			throw new IllegalArgumentException("Illegal extra space character at the start/end of the line ["
			            + line + "]");

		if (line == null || line.length() == 0) //集合为空返回null
			return null;

		final String[] ss = line.split("\t");   //制表符分割语句
		for (int i = 0; i < ss.length; i++)		
		{
			if (ss[i].trim().length() != ss[i].length())
				throw new IllegalArgumentException("Illegal extra space character at the start/end of string ["
				            + ss[i] + "]");
		}

		final int l = calLevel(ss);//计算出现“-”次数，默认为1
		if (l == level) //级别如果与形参相同
		{
			String line2 = reader.take();
			// log.debug("l == level = " + level + ", so take: " + line2);
			if (ss.length < level) //一行比较语句中，制表符分割后的字符串个数小于形参level，抛出异常
				throw new IllegalArgumentException("Not enough arguments: " + line);
			else if ("AND".equalsIgnoreCase(ss[level - 1]) || "OR".equalsIgnoreCase(ss[level - 1]))
			{  //ss在形参level-1位，出现AND或OR字符时，ss的字符串个数应等于形参level
				if (ss.length > level)
					throw new IllegalArgumentException("Too many arguments: " + line);

				final OP op = "AND".equalsIgnoreCase(ss[level - 1]) ? OP.AND : OP.OR;//转换AND或OR为比较OP
				LinkedList<MatchFunction> list = null;
				// log.debug("Com: going deeper.");
				for (MatchFunction mf = parseSingle(reader, level + 1); mf != null; mf = parseSingle(reader,
				            level + 1))
				{
					if (list == null)
					{
						list = new LinkedList<MatchFunction>();
					}
					list.add(mf);//将匹配出的MatchFunction加入集合
				}
				// log.debug("Com: going shallower.");
				//返回构造完成的MatchFunction集合
				return new ComMatchFunction(level, op, list.toArray(new MatchFunction[list.size()]));
			}
			else
			{   //在ss在形参level-1位，未出现AND或OR字符时，ss的字符串个数应为level+2或level+3个
				if (ss.length < level + 2)
					throw new IllegalArgumentException("Not enough arguments: " + line);
				else if (ss.length > level + 3)
					throw new IllegalArgumentException("Too many arguments: " + line);

				final String str_1 = ss[level + 1];
				final String str_2 = level + 2 < ss.length ? ss[level + 2] : null;
				//ss的字符串个数为level+3时，取ss的最后一位

				SimMatchFunction smf = null;
				try
				{   //构建单元级的比较函数，构建过程在TRFieldEnum中
					smf = TRFieldEnum.valueOf(ss[level]).parseFromString(level, str_1, str_2);
				}
				catch (final Exception e)
				{
					throw new IllegalArgumentException(e.getMessage());
				}
				return smf;
			}
		}
		else if (l < level)//字符串中“-”出现次数少于形参，返回空
		{
			// log.debug("l < level, gonna back.");
			return null;
		}
		else		//字符串中“-”出现次数大于形参，抛出异常
		{
			throw new IllegalArgumentException(String.format("Illegal level: %s, l=%d, level=%d", line, l, level));
		}
	}

	/**
	 * Calculate the level of MatchFunction which a string will be parsed to.
	 * 计算级别 默认为1，每个字符串中包含"-"时，这个level加1
	 * @param ss the string
	 * @return the corresponding level
	 */
	private static int calLevel(final String[] ss)
	{
		int index = 1;
		for (final String element : ss)
		{
			if ("-".equals(element))
			{
				index++;
			}
			else
			{
				break;
			}
		}
		return index;
	}

	/**
	 * A assistant class to parse string to MatchFunction.
	 * 
	 * @author SDU.lwlin
	 *
	 */
	private static class PeekLineReader
	{
		private LinkedList<String> list = new LinkedList<String>();//为何不使用queue?

		public PeekLineReader(final String str)
		{
			if (str != null || str.trim().length() > 0)
			{
				for (String s : str.split("\n")) //换行分割字符串
				{
					s = s.trim();				 //去空格
					if (s.length() > 0)			 //加入集合
						list.add(s);
				}
			}
		}

		/**
		 * Get the first line.
		 * 
		 * @return
		 */
		public String peek()	//返回头部元素
		{
			if (list.size() > 0)
				return list.get(0);
			else
				return null;
		}

		/**
		 * Get the first line and remove it from queue.
		 * 
		 * @return
		 */
		public String take()   //移除并返回头部元素
		{
			if (list.size() > 0)
			{
				String str = list.get(0);
				list.remove(0);
				return str;
			}
			else
				return null;
		}

		public static void main(final String[] args) throws Exception
		{
			final PeekLineReader pReader = new PeekLineReader(" 1\n \n\n\n2\n3  \n4\n5\n");
			System.out.println(pReader.take());
			System.out.println(pReader.take());
			System.out.println(pReader.take());
			System.out.println(pReader.take());
			System.out.println(pReader.take());
			System.out.println(pReader.take());
			System.out.println(pReader.take());
			System.out.println(pReader.take());
		}
	}

	public static void main(final String[] args) throws Exception
	{
		// final SimpleDateFormat format = new
		// SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		// final SimMatchFunction line_2 = new SimMatchFunction(2, null,
		// TRFieldEnum.TIMESTAMP, format.parse("2012-09-01 08:00:00 000")
		// .getTime(), format.parse("2012-09-01 09:00:00 000").getTime());
		// System.out.println(line_2);
		// final SimMatchFunction line_5 = new SimMatchFunction(2, null,
		// TRFieldEnum.CAR_PLATE_TYPE,
		// CarPlateType.������Ħ�г�����.ordinal(), null);
		// System.out.println(line_5);
	}
	/*
	 * SimMatchFunction line_3 = new SimMatchFunction(2, null,
	 * TRFieldEnum.CAR_STATUS, CarStatus.ѹ��.ordinal(), null);
	 * SimMatchFunction line_4 = new SimMatchFunction(2, null,
	 * TRFieldEnum.CAR_PLATE_NUMBER, "³A.*", Pattern.compile("³A.*"));
	 * SimMatchFunction line_5 = new SimMatchFunction(2, null,
	 * TRFieldEnum.CAR_PLATE_TYPE, CarPlateType.������Ħ�г�����.ordinal(),
	 * null); SimMatchFunction line_6 = new SimMatchFunction(2, null,
	 * TRFieldEnum.SPEED, 0, 100); SimMatchFunction line_7 = new
	 * SimMatchFunction(2, null, TRFieldEnum.CAR_PLATE_COLOR,
	 * CarPlateColor.��ɫ.ordinal(), null); SimMatchFunction line_8 = new
	 * SimMatchFunction(2, null, TRFieldEnum.CAMERA_LOCATION,
	 * CameraLocation.CL007.ordinal(), null); SimMatchFunction line_9 = new
	 * SimMatchFunction(2, null, TRFieldEnum.TRAVEL_ORIENTATION,
	 * TravelOrientation.�ɶ�����.ordinal(), null); SimMatchFunction line_10 =
	 * new SimMatchFunction(2, null, TRFieldEnum.CAMERA_ORIENTATION,
	 * CameraOrientation.ץ��ͷ.ordinal(), null); SimMatchFunction line_11 = new
	 * SimMatchFunction(2, null, TRFieldEnum.CAR_COLOR, CarColor.��.ordinal(),
	 * null); SimMatchFunction line_12 = new SimMatchFunction(2, null,
	 * TRFieldEnum.CAR_BRAND, CarBrand.AUDI.ordinal(), null); SimMatchFunction
	 * line_13 = new SimMatchFunction(2, null, TRFieldEnum.BAY_ID,
	 * BAYID.BAY002.ordinal(), null); ComMatchFunction line_1 = new
	 * ComMatchFunction(1, OP.AND, new MatchFunction[] { line_2, line_3,
	 * line_4, line_5, line_6, line_7, line_8, line_9, line_10, line_11,
	 * line_12, line_13 });
	 * 
	 * String cmd = line_1.toString(); System.out.println(cmd);
	 * System.out.println(parse(cmd));
	 * 
	 * TravelRecord tr = new TravelRecord();
	 * System.out.print(line_2.toString()); tr.setValue(TRFieldEnum.TIMESTAMP,
	 * format.parse("2012-09-01 08:30:00 0000").getTime());
	 * System.out.println(line_2.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.TIMESTAMP,
	 * format.parse("2012-09-01 07:30:00 0000").getTime());
	 * System.out.println(line_2.match(tr.toBytes()));
	 * 
	 * System.out.print(line_3.toString());
	 * tr.setValue(TRFieldEnum.CAR_STATUS, CarStatus.ѹ��);
	 * System.out.println(line_3.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAR_STATUS, CarStatus.����);
	 * System.out.println(line_3.match(tr.toBytes()));
	 * 
	 * System.out.print(line_4.toString());
	 * tr.setValue(TRFieldEnum.CAR_PLATE_NUMBER, "³A12345");
	 * System.out.println(line_4.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAR_PLATE_NUMBER, "³B12345");
	 * System.out.println(line_4.match(tr.toBytes()));
	 * 
	 * System.out.print(line_5.toString());
	 * tr.setValue(TRFieldEnum.CAR_PLATE_TYPE, CarPlateType.������Ħ�г�����);
	 * System.out.println(line_5.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAR_PLATE_TYPE, CarPlateType.��ʱ��ʻ������);
	 * System.out.println(line_5.match(tr.toBytes()));
	 * 
	 * System.out.print(line_6.toString()); tr.setValue(TRFieldEnum.SPEED,
	 * 80); System.out.println(line_6.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.SPEED, 150);
	 * System.out.println(line_6.match(tr.toBytes()));
	 * 
	 * System.out.print(line_7.toString());
	 * tr.setValue(TRFieldEnum.CAR_PLATE_COLOR, CarPlateColor.��ɫ);
	 * System.out.println(line_7.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAR_PLATE_COLOR, CarPlateColor.������ɫ);
	 * System.out.println(line_7.match(tr.toBytes()));
	 * 
	 * System.out.print(line_8.toString());
	 * tr.setValue(TRFieldEnum.CAMERA_LOCATION, CameraLocation.CL007);
	 * System.out.println(line_8.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAMERA_LOCATION, CameraLocation.CL008);
	 * System.out.println(line_8.match(tr.toBytes()));
	 * 
	 * System.out.print(line_9.toString());
	 * tr.setValue(TRFieldEnum.TRAVEL_ORIENTATION, TravelOrientation.�ɶ�����);
	 * System.out.println(line_9.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.TRAVEL_ORIENTATION, TravelOrientation.�ɱ�����);
	 * System.out.println(line_9.match(tr.toBytes()));
	 * 
	 * System.out.print(line_10.toString());
	 * tr.setValue(TRFieldEnum.CAMERA_ORIENTATION, CameraOrientation.ץ��ͷ);
	 * System.out.println(line_10.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAMERA_ORIENTATION, CameraOrientation.ץ��β);
	 * System.out.println(line_10.match(tr.toBytes()));
	 * 
	 * System.out.print(line_11.toString());
	 * tr.setValue(TRFieldEnum.CAR_COLOR, CarColor.��);
	 * System.out.println(line_11.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAR_COLOR, CarColor.����);
	 * System.out.println(line_11.match(tr.toBytes()));
	 * 
	 * System.out.print(line_12.toString());
	 * tr.setValue(TRFieldEnum.CAR_BRAND, CarBrand.AUDI);
	 * System.out.println(line_12.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.CAR_BRAND, CarBrand.SANTANA);
	 * System.out.println(line_12.match(tr.toBytes()));
	 * 
	 * System.out.print(line_13.toString()); tr.setValue(TRFieldEnum.BAY_ID,
	 * BAYID.BAY002); System.out.println(line_13.match(tr.toBytes()));
	 * tr.setValue(TRFieldEnum.BAY_ID, BAYID.BAY007);
	 * System.out.println(line_13.match(tr.toBytes())); }
	 */
}