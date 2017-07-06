package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction.OP;

public class Speed extends TRFieldBase{
	
	public  Speed(String name, int fixSize,int OFFSET) {
		super(name, fixSize,OFFSET);
	}
	
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		int s = ByteUtil.getShort(bs, OFFSET);
		return s >= (Integer) value_1 && s <= (Integer) value_2;
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		int speed = obj == null ? 0 : (Integer) obj;
		ByteUtil.putShort(bs, (short) speed, OFFSET);
	}

	@Override
	public Object readBytes(byte[] bs) {
		return ByteUtil.getShort(bs, OFFSET);
	}

	@Override
	public String[] printToString(Object value_1, Object value_2) {
		return new String[] { Integer.toString((Integer) value_1),
				Integer.toString((Integer) value_2) };
	}

	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		return new SimMatchFunction(level, OP.BETWEEN, this,
				Integer.valueOf(str_1), Integer.valueOf(str_2));
	}
}
