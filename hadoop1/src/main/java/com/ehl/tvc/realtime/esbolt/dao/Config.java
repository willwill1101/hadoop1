
package com.ehl.tvc.realtime.esbolt.dao;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/08/21
 * Time: 13:42
 */
public interface Config {
    public String CLUSTER_NAME="cluster.name";
    public String CLUSTER_IPS_PORTS="cluster.ipandports";
    public String CLUSTER_INDEX="cluster.index";
    public String CLUSTER_TYPE="cluster.type";
    public String RESPONSE_CONTENT_TYPE="response.content.type";
    public String CLIENT_TRANSPORT_SNIFF="client.transport.sniff";
    public String BULK_ACTIONS="bulk.actions";
    public String BULK_SIZE="bulk.size";
    public String FLUSH_INTERVAL="flush.interval";
}
