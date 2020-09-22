package com.supershan.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;


/**
 * @author mac
 */
public class ClientUtil {
    private static TransportClient client;
    public TransportClient createClient() throws Exception {
        // 先构建client
        System.out.println("开始创建client");
        Settings settings=Settings.builder()
                .put("cluster.name","elasticsearch")
                .put("client.transport.ignore_cluster_name", true)
                .build();

        //如果集群名不对，也能连接
        //创建Client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(
                        new TransportAddress(
                                InetAddress.getByName(
                                        "localhost"),
                                9300));
        return client;
    }
}
