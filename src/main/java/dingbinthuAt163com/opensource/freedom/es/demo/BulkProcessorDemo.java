package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 15:49
 */
public class BulkProcessorDemo {
    private static Logger LOG = LoggerFactory.getLogger(BulkProcessorDemo.class);

    public static void main(String[] args) {
        try {
            TransportClient client = EsClient.getClient();


            BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    LOG.info("executionId:" + executionId + ",request number:" + request.numberOfActions() + ",request size:"
                            + request.estimatedSizeInBytes()  + " Bytes.");
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    if (response.hasFailures()) {
                        LOG.error("executionId:" + executionId + ",response has failures:" + response.hasFailures()
                                + ",response content:" + response.buildFailureMessage());
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    LOG.error("executionId:" + executionId + ", throwable:" + ExceptionUtil.getTrace(failure));

                }
            })
                    .setBulkActions(20000)
                    .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
//                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(4)
                    .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50),8))
                    .build();

             // Add your requests
            bulkProcessor.add(new IndexRequest("a", "b", "111").source(jsonBuilder()
                    .startObject()
                    .field("a","xxxxx")
                    .field("b","xxxxx")
                    .field("z","xxxxx")
                    .endObject()
            ));

             // Flush any remaining requests
            bulkProcessor.flush();

             // Or close the bulkProcessor if you don't need it anymore
//            bulkProcessor.close();
            bulkProcessor.awaitClose(24,TimeUnit.HOURS);

            // Refresh your indices
            client.admin().indices().prepareRefresh().get();

            // Now you can start searching!
            client.prepareSearch().get();



            // client.close();
        }
        catch (Exception ex) {
            LOG.error(ExceptionUtil.getTrace(ex));
        }
    }
}
