package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 15:49
 */
public class SearchScrollApiDemo {
    private static Logger LOG = LoggerFactory.getLogger(SearchScrollApiDemo.class);

    public static void main(String[] args) {
        try {
            TransportClient client = EsClient.getClient();

//            QueryBuilder qb = termQuery("multi", "test");
            QueryBuilder qb = null;

            SearchResponse searchResponse = client.prepareSearch("a")
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                    .setScroll(new TimeValue(60000))
                    .setQuery(qb)
                    .setSize(100).get(); //max of 100 hits will be returned for each scroll
                                        //Scroll until no hits are returned
            do {
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    LOG.info(hit.toXContent(jsonBuilder().prettyPrint(),null).string());
                }

                searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            } while(searchResponse.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.

            // client.close();
        }
        catch (Exception ex) {
            LOG.error(ExceptionUtil.getTrace(ex));
        }
    }
}
