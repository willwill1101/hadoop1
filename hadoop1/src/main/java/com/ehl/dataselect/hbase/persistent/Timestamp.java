package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction.OP;
import com.ehl.dataselect.hbase.util.DateFormatter;

public class Timestamp extends TRFieldBase{
	
	public Timestamp(String name, int fixSize,int OFFSET){
		super(name, fixSize,OFFSET);
	}
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		long time = ByteUtil.getLong(bs, OFFSET);
		return time >= (Long) value_1 && time <= (Long) value_2;
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		long ts = obj == null ? 0 : (Long) obj;
		ByteUtil.putLong(bs, ts, OFFSET);
	}

	@Override
	public Object readBytes(byte[] bs) {
		return ByteUtil.getLong(bs, OFFSET);
	}

	@Override
	public String[] printToString(Object value_1, Object value_2) {
		return new String[] { DateFormatter.format((Long) value_1),
				DateFormatter.format((Long) value_2) };
	}

	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		long t1 = DateFormatter.parse(str_1, -1);
		if (t1 < 0)
			throw new IllegalArgumentException(String.format(
					MatchFunction.ERR_STR_ILL_ARGUMENT, str_1,
					this.toString() + "_1"));
		long t2 = DateFormatter.parse(str_2, -1);
		if (t2 < 0)
			throw new IllegalArgumentException(String.format(
					MatchFunction.ERR_STR_ILL_ARGUMENT, str_1,
					this.toString() + "_2"));
		return new SimMatchFunction(level, OP.BETWEEN, this, (Long) t1,
				(Long) t2);
	}

	@Override
	public String toTRtoString(Object value) {
		long ts = (Long) value;
		return super.toTRtoString(DateFormatter.format(ts));
	}
}
