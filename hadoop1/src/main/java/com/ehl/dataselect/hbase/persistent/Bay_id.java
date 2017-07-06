package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction.OP;

public class Bay_id extends TRFieldBase{
	
	public  Bay_id(String name, int fixSize, boolean elike,int OFFSET) {
		super(name, fixSize, elike,OFFSET);
	}
	
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		return ByteUtil.getShort(bs, OFFSET) == Integer.parseInt((String)value_1);
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		int i = obj == null ? 0 :Integer.parseInt((String)obj);
		ByteUtil.putShort(bs, (short) i, OFFSET);
	}

	@Override
	public Object readBytes(byte[] bs) {
		return ByteUtil.getShort(bs, OFFSET);
	}


	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		try {
			return new SimMatchFunction(level, OP.IS, this,str_1, null);
		} catch (Exception e) {
			e.printStackTrace();
			return new SimMatchFunction(level, OP.IS, this, 0, null);
			// throw new
			// IllegalArgumentException(String.format(MatchFunction.ERR_STR_ILL_ARGUMENT,
			// e.getMessage()));
		}
	}
}
