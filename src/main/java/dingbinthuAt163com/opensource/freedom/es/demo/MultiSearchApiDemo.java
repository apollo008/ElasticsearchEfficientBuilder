package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 15:49
 */
public class MultiSearchApiDemo {
    private static Logger LOG = LoggerFactory.getLogger(MultiSearchApiDemo.class);

    public static void main(String[] args) {
        try {
            TransportClient client = EsClient.getClient();
            SearchRequestBuilder srb1 = client
                    .prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(100).setFrom(0);
            SearchRequestBuilder srb2 = client
                    .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(10).setFrom(0);

            MultiSearchResponse sr = client.prepareMultiSearch()
                    .add(srb1)
                    .add(srb2)
                    .get();

           // You will get all individual responses from MultiSearchResponse#getResponses()
            long nbHits = 0;
            for (MultiSearchResponse.Item item : sr.getResponses()) {
                SearchResponse response = item.getResponse();
                nbHits += response.getHits().getTotalHits();
            }
            LOG.info("total hits:" + nbHits);

            // client.close();
        }
        catch (Exception ex) {
            LOG.error(ExceptionUtil.getTrace(ex));
        }
    }
}
