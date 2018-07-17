package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 23:32
 */
public class EsAdminApiDemo {
    private static Logger LOG = LoggerFactory.getLogger(EsAdminApiDemo.class);

    public static void main(String[] args) {
        try {
            TransportClient client = EsClient.getClient();
            AdminClient adminClient = client.admin();

            // Refresh your indices
            adminClient.indices().prepareRefresh().get();
        }
        catch (Exception ex) {
            LOG.error(ExceptionUtil.getTrace(ex));
        }
    }
}
