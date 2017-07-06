/**
 * 
 */
package com.ehl.dataselect.hbase.persistent;

import java.util.Arrays;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;


public class Plate_coordinates   extends TRFieldBase{
	
	public Plate_coordinates(String name, int fixSize,int OFFSET) {
		super( name,  fixSize, OFFSET);
	}
	
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		// always return false for unsupported match
		return false;
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		if (obj == null)
			obj = new int[4];
		ByteUtil.putShort(bs, (short) ((int[]) obj)[0], OFFSET);
		ByteUtil.putShort(bs, (short) ((int[]) obj)[1], OFFSET + 2);
		ByteUtil.putShort(bs, (short) ((int[]) obj)[2], OFFSET + 4);
		ByteUtil.putShort(bs, (short) ((int[]) obj)[3], OFFSET + 6);
	}

	@Override
	public Object readBytes(byte[] bs) {
		int[] is = new int[4];
		for (int i = 0; i < 4; i++)
			is[i] = ByteUtil.getShort(bs, OFFSET + i * 2);
		return is;
	}

	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		return null;
	}

	@Override
	public String toTRtoString(Object value) {
		return super.toTRtoString(Arrays.toString((int[]) value));
	}
}