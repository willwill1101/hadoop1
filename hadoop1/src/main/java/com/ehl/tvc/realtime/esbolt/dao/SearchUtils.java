package com.ehl.tvc.realtime.esbolt.dao;/*


import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import tv.icntv.search.utils.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/11/13
 * Time: 16:19
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import com.yang.hadoop1.ContentUtil;
import com.yang.hbase2es.Hbase2es;


public class SearchUtils implements  Config {
	private static final Logger log = Logger.getLogger(SearchUtils.class);
    private static SearchUtils search = new SearchUtils();
    private TransportClient client = null;
    private BulkProcessor bulkProcessor = null;
    public static SearchUtils getInstance(){
    	ContentUtil.init(SearchUtils.class.getClassLoader().getResourceAsStream("conf.properties"));
       return search;
    }

    private SearchUtils() {
    }

    public TransportClient getESClient() {
    	if(client==null) {
    		List<InetSocketTransportAddress> list = getInetSocket();
            if(null == list || list.isEmpty()){
                return null;
            }
            //初始化client
            Settings settings = Settings.settingsBuilder().put(CLUSTER_NAME,ContentUtil.getValue(CLUSTER_NAME)).put(CLIENT_TRANSPORT_SNIFF,ContentUtil.getValue(CLIENT_TRANSPORT_SNIFF)).build();
			client = TransportClient.builder().settings(settings).build().addTransportAddresses(list.toArray(new InetSocketTransportAddress[list.size()]));
    	}
        return client;
    }
    
    /**
     * 提交请求给批量处理
     * @param request
     */
    public void bulkProcessor(ActionRequest request){
    	getBulkProcessor(getESClient()).add(request);
    }
    
    
    private BulkProcessor getBulkProcessor(TransportClient clinet){
    	//初始化bulkProcessor
    	if(bulkProcessor==null){
		 bulkProcessor = BulkProcessor.builder(
		        client, 
		        new BulkProcessor.Listener() {
		            @Override
		            public void beforeBulk(long executionId,
		                                   BulkRequest request) {
		            }

		            @Override
		            public void afterBulk(long executionId,
		                                  BulkRequest request,
		                                  BulkResponse response) {
		            	log.info("本次批量提交es请求数："+request.numberOfActions()+",耗时毫秒："+response.getTookInMillis()+",是否部分失败："+response.hasFailures());
		            }

		            @Override
		            public void afterBulk(long executionId,
		                                  BulkRequest request,
		                                  Throwable failure) {
		            	log.error("es写入异常", failure);
		            } 
		        })
		        .setBulkActions(Integer.valueOf(ContentUtil.getValue(BULK_ACTIONS))) 
		        .setBulkSize(new ByteSizeValue(Integer.valueOf(ContentUtil.getValue(BULK_SIZE)), ByteSizeUnit.MB)) 
		        .setFlushInterval(TimeValue.timeValueSeconds(Integer.valueOf(ContentUtil.getValue(FLUSH_INTERVAL)))) 
		        .setConcurrentRequests(1)
		        .setBackoffPolicy(
		        		Boolean.valueOf(ContentUtil.getValue(FLUSH_INTERVAL))?
		            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3):BackoffPolicy.noBackoff())
		        .build();
    	}
    	return bulkProcessor;
    }

    private List<InetSocketTransportAddress> getInetSocket(){
        List<InetSocketTransportAddress> list = new ArrayList<InetSocketTransportAddress>();
        String ips = ContentUtil.getValue(CLUSTER_IPS_PORTS);
        if(ips==null || ips.trim().equals("")){
            return null;
        }
        for(String line : ips.split(",")){
            String[]temps = line.split(":");
            try {
				list.add(new InetSocketTransportAddress(InetAddress.getByName(temps[0]),Integer.parseInt(temps[1])));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
        }
        return list;
    }




}
