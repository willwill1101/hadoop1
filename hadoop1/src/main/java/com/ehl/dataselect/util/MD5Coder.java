package com.ehl.dataselect.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class is used to encode the urls of images.
 * 加密管理类，用于处理图片url
 * 
 * @author SDU.Dong
 * 
 */
public class MD5Coder {

	private static MessageDigest md = null;

	static {
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// we know this step couldn't be reached
			System.exit(1);
		}
	}

	/**
	 * Cast a byte value to a char value.
	 * 
	 * @param b
	 *            the byte value
	 * @param left
	 *            the lower bound
	 * @param right
	 *            the upper bound
	 * @return the corresponding result
	 */
	private static char byte2char(byte b, int left, int right) {
		int intvalue = b & 0xFF;
		int modvalue = right - left + 1;
		return (char) (intvalue % modvalue + left);
	}

	/**
	 * Cast a byte value to an upper case character.
	 * 
	 * @param b
	 *            the byte value
	 * @return the corresponding result
	 */
	private static char byte2UC(byte b) {
		return byte2char(b, 65, 90);
	}

	/**
	 * Cast a byte value to an lower case character.
	 * 
	 * @param b
	 *            the byte value
	 * @return the corresponding result
	 */
	private static char byte2LC(byte b) {
		return byte2char(b, 97, 122);
	}

	/**
	 * Cast a byte value to a character whose value is a digit.
	 * 
	 * @param b
	 *            the byte value
	 * @return the corresponding result
	 */
	private static char byte2Dig(byte b) {
		return byte2char(b, 48, 57);
	}

	/**
	 * 字符串加密
	 * 
	 * @param str
	 *            the string value
	 * @return the corresponding result
	 */
	public static synchronized String getCode(String str) {
		if (str == null)
			return null;
		byte[] input = null;
		try {
			input = str.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		md.update(input);
		byte[] inter = md.digest();
		byte[] secinput = new byte[6];
		secinput[0] = inter[1];
		secinput[1] = inter[2];
		secinput[2] = inter[3];
		secinput[3] = inter[5];
		secinput[4] = inter[8];
		secinput[5] = inter[13];
		md.update(secinput);
		byte[] resbs = md.digest();
		char[] reschar = new char[6];
		reschar[0] = byte2LC(resbs[1]);
		reschar[1] = byte2Dig(resbs[3]);
		reschar[2] = byte2UC(resbs[7]);
		reschar[3] = byte2LC(resbs[15]);
		reschar[4] = byte2UC(resbs[5]);
		reschar[5] = byte2Dig(resbs[8]);
		String res = new String(reschar);
		return res;
	}
}
