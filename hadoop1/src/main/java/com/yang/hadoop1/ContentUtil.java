package com.yang.hadoop1;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hello world!
 *
 */
public class ContentUtil 
{	
	private static	Logger log = LoggerFactory.getLogger(ContentUtil.class);
	private static  Properties properties = new Properties();
	public static List<String> fieldPaths =  new ArrayList<String>();
	public static List<String> fieldImagePaths =  new ArrayList<String>();
	public static List<String> imagePaths =  new ArrayList<String>();
	
   static{

	   InputStream input = null;
	   try {
		input=new FileInputStream("../conf/conf.properties");
		Reader reader =  new InputStreamReader(input, "UTF-8");
		   properties.load(reader);
		   for(Entry<Object, Object> entry : properties.entrySet()){
		   }
		   log.info("配置文件加载成功 ！");
	} catch (IOException e) {
		 log.debug("配置文件加载失败！",e);
	}finally {
		if(input!=null){
			try {
				input.close();
			} catch (IOException e) {
			}
		}
	}
   }
   
   
   
   /**
    * 获取配置文件
    * @param key
    * @return
    */
    public static  String getValue(String key){
    	return properties.getProperty(key.toString());
    }
   
}
