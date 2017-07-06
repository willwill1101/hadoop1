package com.ehl.tvc.realtime.esbolt.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ehl.tvc.realtime.db.DBPool;
import com.yang.hadoop1.ContentUtil;
import com.yang.hbase2es.Hbase2es;

/**
 * 字典数据同步服务
 * @author lijun
 *2015年11月10日
 */
public class DictSynchronizeService {
	private static DictSynchronizeService instance = null;
	private static final String HPSL_DMLB = "261009"; //字典表中号牌缩略类型代码类别编码
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private Map<String, String> cacheMap = new HashMap<String, String>();
	
	public synchronized static DictSynchronizeService getInstance() {
		if (null ==instance) {
			instance = new DictSynchronizeService();
			instance.updateCacheMap();
		}
		return instance;
	}
	
	private DictSynchronizeService() {
		super();
	}

	/**
	 * 判断所给字符串是否为车牌前缀
	 * @param slStr
	 * @return
	 */
	public boolean isPreFix(String slStr) {
		String type =  cacheMap.get(slStr);
		if(type ==null){
			return false;
		}
		return true;
	}


	private void updateCacheMap() {
		ContentUtil.init(Hbase2es.class.getClassLoader().getResourceAsStream("conf.properties"));
			String sql = "select dm,xh from t_itgs_code	where dmlb=?";
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				DBPool.initDBPool("ehl", ContentUtil.getMap());
				conn = DBPool.getConnection();
				ps = conn.prepareStatement(sql);
				ps.setString(1, HPSL_DMLB);
				rs = ps.executeQuery();
				//cacheMap.clear();
				Map<String, String> cacheMapTmp=new HashMap<String, String>();
				while (rs.next()) {
					cacheMapTmp.put(rs.getString(1),"");
				}
				if(cacheMapTmp.size()>0){
					cacheMap = cacheMapTmp;
				}
				log.debug("号牌缩略-号牌缩略编码缓存更新成功");
			} catch (SQLException e) {
				e.printStackTrace();
				log.debug("号牌缩略-号牌缩略编码缓存更新失败",e);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				if (ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
	
	}
}
