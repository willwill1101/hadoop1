package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.MatchFunction;
import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;

public class Version extends TRFieldBase{
	
		public  Version(String name, int fixSize,int OFFSET) {
//			super("version",1);
			super(name, fixSize,OFFSET);
		}
		
		@Override
		public boolean match(byte[] bs, Object value_1, Object value_2) {
			return false;
		}

		@Override
		public void fillBytes(byte[] bs, Object obj) {
			int version = obj == null ? 2 : (Integer) obj;
			ByteUtil.putTinyInt(bs, version, OFFSET);
		}

		@Override
		public Object readBytes(byte[] bs) {
			return ByteUtil.getTinyInt(bs, OFFSET);
		}

		@Override
		public SimMatchFunction parseFromString(int level, String value_1,
				String value_2) throws IllegalArgumentException {
			throw new IllegalArgumentException(String.format(
					MatchFunction.ERR_STR_NOT_SUPPORTED, this.toString()));
		}

		@Override
		public String toTRtoString(Object value) {
			return super.toTRtoString(value);
		}
}
