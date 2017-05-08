package com.yang.hadoop1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

/**
  * @ClassName: HTableProxy 
  * @Description: hbase连接词 
  * @author 卢海友 
  * @date 2015-3-26 上午9:54:44
 */
public class HTableProxy {
	private static Logger logger = Logger.getLogger(HTableProxy.class);
	private static HTablePool pool = null;
	private static Configuration conf;
	private static boolean init;
	public static void init(){
		//初始化HBase链接
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "host2,host3,host4,host14,host15");				// 设置zooKeeper访问地址
			pool = new HTablePool(conf, 300); 	
			init = true;
	}
	public static void reset(){

		if(pool != null){
			try {
				pool.close();
			} catch (IOException e) {
				logger.error("Htable线程池关闭失败：" + e.getMessage());
				e.printStackTrace();
			}
		}
		pool = new HTablePool(conf, 1000);
	
	}
	
	/**
	 * @param name
	 * @return
	 */
	public static HTable getHTable(String tableName){
		if(!init){
			init();
		}
		HTable table = (HTable) pool.getTable(tableName); //从HTable池中获取链接
		table.setAutoFlush(false);
		try {
			table.setWriteBufferSize(0x500000L);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*synchronized (pool) {
			table = (HTable) pool.getTable(tableName); //从HTable池中获取链接
		}*/
		return table;
	}
}

