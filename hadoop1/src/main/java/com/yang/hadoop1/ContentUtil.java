package com.yang.hadoop1;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
	public static List<String> cloudids =  new ArrayList<String>();
	
   public static void init (String path){

	   	try {
			InputStream input = null;
			input=new FileInputStream(path+"/conf.properties");
			init ( input);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   
   }
   
   public static void init (InputStream input){

	   try {
		   properties.load(input);
		   log.info("配置文件加载成功！");
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
    
    
    public static Map<String,String > getMap(){
    	Set set = properties.keySet();
    	Map<String ,String> map =  new HashMap<String ,String>(); 
    	for(Object o : set){
    		Object v = properties.get(o);
    		map.put(o.toString(), v.toString());
    	}
    	return map;
    }
   
}
