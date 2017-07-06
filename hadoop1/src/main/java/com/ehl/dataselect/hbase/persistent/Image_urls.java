/**
 * 
 */
package com.ehl.dataselect.hbase.persistent;

import java.util.Arrays;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;


public class Image_urls   extends TRFieldBase{
	
	public Image_urls(String name, int fixSize,int OFFSET) {
		super( name,  fixSize, OFFSET);
	}
	
	@Override
	public boolean match(byte[] bs, Object value_1, Object value_2) {
		// always return false for unsupported match
		return false;
	}

	@Override
	public void fillBytes(byte[] bs, Object obj) {
		String img_1 = null;
		String img_2 = null;
		String img_3 = null;
		if (obj != null) {
			img_1 = ((String[]) obj)[0];
			img_2 = ((String[]) obj)[1];
			img_3 = ((String[]) obj)[2];
		}
		int offset = OFFSET;
		offset = ByteUtil.putString(bs, img_1, offset);
		offset = ByteUtil.putString(bs, img_2, offset);
		offset = ByteUtil.putString(bs, img_3, offset);
	}

	@Override
	public Object readBytes(byte[] bs) {
		String[] imgUrls = new String[3];
		byte[] tmp = new byte[256];
		int index = OFFSET;
		for (int i = 0; i < 3; i++) {
			String str = ByteUtil.getString(bs, index);
			index = ByteUtil.putString(tmp, str, index);
			imgUrls[i] = str.length() == 0 ? null : str;
		}
		return imgUrls;
	}


	@Override
	public SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toTRtoString(Object value) {
		return super.toTRtoString(Arrays.toString((String[]) value));
	}
}