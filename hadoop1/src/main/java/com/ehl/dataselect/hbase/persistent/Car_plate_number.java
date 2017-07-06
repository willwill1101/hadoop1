package com.ehl.dataselect.hbase.persistent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction.OP;


public class Car_plate_number extends TRFieldBase{

	public  Car_plate_number(String name, int fixSize,int OFFSET) {
//		super("version",1);
		super(name, fixSize,OFFSET);
	}
	
//	 value_1 as string, value_2 as Pattern
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		Matcher m = ((Pattern) value_2).matcher((String) readBytes(bs));
		return m.matches();
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		ByteUtil.putFixedString(bs, (String) obj, OFFSET);
	}

	@Override
	public Object readBytes(byte[] bs) {
		String str = null;
		int version = 2;
		try {
			version = (Integer) TRFieldEnum.VERSION.readBytes(bs);
		} catch (Exception e) {
		}
		if (version == 1)
			str = ByteUtil.getString(bs, OFFSET);
		else
			str = ByteUtil.getFixedString(bs, OFFSET);
		return str.length() == 0 ? null : str;
	}


	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		str_1 = str_1.replaceAll("_", ".").replaceAll("%", ".*");
		return new SimMatchFunction(level, OP.MATCH, this, str_1,
				Pattern.compile(str_1));
	}
}
