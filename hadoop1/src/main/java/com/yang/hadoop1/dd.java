package com.yang.hadoop1;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class dd {
	private static String hexStr =  "0123456789ABCDEF"; 
	public static void main(String[] args) throws ParseException {
		 DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		byte[] rowkey = new byte[22];
		ByteUtil.putString(rowkey, "无牌",  0);
		System.out.println(bin2HexStr(rowkey));
		ByteUtil.putLong(rowkey, df.parse("2017-03-01 08:00:00").getTime(), 12);
		System.out.println(bin2HexStr(rowkey));
		ByteUtil.putShort(rowkey, Short.parseShort("0"), 20);
		System.out.println(bin2HexStr(rowkey));
	}
	
	 /** 
     *  
     * @param bytes 
     * @return 将二进制数组转换为十六进制字符串  2-16
     */  
    public static String bin2HexStr(byte[] bytes){  

        String result = "";  
        String hex = "";  
        for(int i=0;i<bytes.length;i++){  
            //字节高4位  
            hex = String.valueOf(hexStr.charAt((bytes[i]&0xF0)>>4));  
            //字节低4位  
            hex += String.valueOf(hexStr.charAt(bytes[i]&0x0F));  
            result +=hex;  //+" "
        }  
        return result;  
    } 
}
