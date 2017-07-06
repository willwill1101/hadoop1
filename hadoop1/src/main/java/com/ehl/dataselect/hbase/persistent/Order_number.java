package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;

public class Order_number extends TRFieldBase{
	
	public Order_number(String name, int fixSize,int OFFSET){
		super(name, fixSize, OFFSET);
	}
	
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		// always return false for unsupported match
		return false;
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		short order = obj == null ? 0 : ((Integer) obj).shortValue();
		ByteUtil.putShort(bs, order, OFFSET);
	}

	@Override
	public Object readBytes(byte[] bs) {
		return ByteUtil.getShort(bs, OFFSET);
	}

	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		throw new IllegalArgumentException(String.format(
				MatchFunction.ERR_STR_NOT_SUPPORTED,
				this.toString()));
	}
}
