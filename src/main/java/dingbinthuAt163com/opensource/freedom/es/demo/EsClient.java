package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 10:35
 */
public class EsClient  {
    private static Logger LOG = LoggerFactory.getLogger(EsClient.class);

    private static Settings settings = Settings.builder()
            .put("cluster.name", "es-cluster")
            .put("client.transport.sniff", true)
            .put("client.transport.ping_timeout", 120, TimeUnit.SECONDS)
            .put("client.transport.nodes_sampler_interval",5, TimeUnit.SECONDS)
            .build();

    private static TransportClient client;

    static {
        try {
            //注意：java 反射 比 直接new一个对象要块，另外单例模式 避免重复new多个对象
            Class<?> clazz = Class.forName(PreBuiltTransportClient.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class, Collection.class);
            constructor.setAccessible(true);
            client = (PreBuiltTransportClient) constructor.newInstance(settings, new ArrayList<>());
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.110"), 19300));
        } catch (Exception e) {
            LOG.error(ExceptionUtil.getTrace(e));
        }
    }

    public static TransportClient getClient() {
        return client;
    }
}
