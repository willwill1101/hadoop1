package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction.OP;

public class Lane_id  extends TRFieldBase{
	
	public  Lane_id(String name, int fixSize,int OFFSET) {

		super(name, fixSize,OFFSET);
	}
	
	
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		return ByteUtil.getInt(bs, OFFSET) == (Integer)value_1;
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		int lid = obj == null ? 0 : (Integer) obj;
		ByteUtil.putInt(bs, lid, OFFSET);
	}

	@Override
	public Object readBytes(byte[] bs) {
		return ByteUtil.getInt(bs, OFFSET);
	}


	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		return new SimMatchFunction(level, OP.IS, this,
				Integer.parseInt(str_1), null);
	}
	
}
