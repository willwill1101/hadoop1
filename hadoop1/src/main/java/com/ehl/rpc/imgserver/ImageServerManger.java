package com.ehl.rpc.imgserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

/**
* @ClassName: ImageServerManger 
* @Description: 获取图片服务器的工具 多个图片服务器循环使用
* @author luhy 
* @date 2014-5-6 下午2:57:00 
* @version V1.0
 */
public class ImageServerManger {
	private static final Logger log = Logger.getLogger(ImageServerManger.class);
	private static final Random rand = new Random();
	private ImageServerManger(){}
	private static final List<String> list = new ArrayList<String>();
	private static final ReadWriteLock lock = new ReentrantReadWriteLock();
	public  static void addImageServer(List<String> paths){
		lock.writeLock().lock();
		try {
			list.clear();
			for(String path : paths){
				list.add("http://"+path);
				log.info("新加入图片服务器地址:"+path);
			}
		}finally{
			lock.writeLock().unlock();
		}
	}
	public  static String getPath(){
		String imgpath = "http://128.127.121.29:9992";
		lock.readLock().lock();
		try {
			if(list.size()>0){
				imgpath = list.get(rand.nextInt(list.size()));
			}
		}finally{
			lock.readLock().unlock();
		}
		return imgpath;
	}
}
