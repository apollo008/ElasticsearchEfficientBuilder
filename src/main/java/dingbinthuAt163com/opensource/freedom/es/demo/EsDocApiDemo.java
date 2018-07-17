package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.err;
import static java.lang.System.out;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-05, 8:12
 */
public class EsDocApiDemo {
    private static Logger LOG = LoggerFactory.getLogger(EsDocApiDemo.class);


    public static void main(String[] args) {
        try {
            TransportClient client = EsClient.getClient();

            //Document APIs Index API
            Map<String, Object> map1 = new HashMap<String, Object>();
            map1.put("user","kimchy");
            map1.put("postDate",new Date());
            map1.put("message","trying out Elasticsearch");

            String json = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";

            IndexResponse response1 = client.prepareIndex("a","b","001").setSource(map1).get();
            out.println(response1.toString());

            IndexResponse response2 = client.prepareIndex("a","b","002").setSource(json, XContentType.JSON).get();
            out.println(response2.toXContent(jsonBuilder(),null).string());

            IndexResponse response3 = client.prepareIndex("a", "b", "003") .setSource(jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject()
            ).get();
            out.println(response3.toXContent(jsonBuilder().prettyPrint(),null).string());


            //Document APIs GET API
            GetResponse getResponse1 = client.prepareGet("a","b","001").get();
            out.println(getResponse1.toString());
            GetResponse getResponse2 = client.prepareGet("a","b","002").setOperationThreaded(false).get();
            out.println(getResponse2.toXContent(XContentFactory.yamlBuilder().prettyPrint(),null).string());



            //Document APIS delete API
            DeleteResponse delResponse1 = client.prepareDelete("twitter", "tweet", "xxx").get();
            out.println(delResponse1.toXContent(jsonBuilder().prettyPrint(),null).string());

            //Document APIS delete by query API
            //way1 , sync way, use get()
            BulkByScrollResponse bbsreponse1 = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                    .filter(QueryBuilders.matchQuery("gender","male"))
//                    .source("_all")
                    .source("a")
                    .get();

            long deleted = bbsreponse1.getDeleted();
            out.println("===deleted :" + deleted);
//            out.println(bbsreponse1.toXContent(XContentFactory.jsonBuilder().prettyPrint(),new Params.EMPTY_PARAMS).string());

            //way2, async way, use listener
//            DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//                    .filter(QueryBuilders.matchQuery("gender", "male"))
//                    .source("a")
//                    .execute(new ActionListener<BulkIndexByScrollResponse>() {
//                        @Override
//                        public void onResponse(BulkIndexByScrollResponse response) {
//                            long deleted = response.getDeleted();
//                            out.println("---------------" + deleted);
//                        }
//                        @Override
//                        public void onFailure(Exception e) {
//                            // Handle the exception
//                            err.println(ExceptionUtil.getTrace(e));
//                        }
//                    });


            //Document APIS update API
            UpdateResponse updateResponse1 = client.prepareUpdate("a", "b", "001")
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("genderxx", "malexxxxxxxxx")
                            .endObject())
                    .get();
            out.println(updateResponse1.toXContent(jsonBuilder().prettyPrint(),null).string());

            UpdateRequest updateRequest = new UpdateRequest("a","b","003").doc( jsonBuilder()
                    .startObject()
                    .field("gender", "male")
                    .endObject() );
            UpdateResponse updateResponse2 = client.update(updateRequest).get();
            out.println(updateResponse2.toXContent(jsonBuilder().prettyPrint(),null).string());


            //DocumentAPIS upsert
            IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                    .source(jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("gender", "male")
                            .endObject());
            UpdateRequest updateRequest2 = new UpdateRequest("index", "type", "3")
                    .doc(jsonBuilder()
                            .startObject()
                            .field("gender", "male")
                            .endObject())
                    .upsert(indexRequest);
            UpdateResponse updateResponse3 = client.update(updateRequest2).get();
            out.println(updateResponse3.toXContent(jsonBuilder().prettyPrint(),null).string());


            //Document APIS multiget api
            MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                    .add("twitter", "tweet", "1")
                    .add("twitter", "tweet", "2", "3", "4")
                    .add("another", "type", "foo")
                    .get();

            out.println("======================================MultiGet as follows:");
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                MultiGetResponse.Failure failure = itemResponse.getFailure();
                if (null != failure)  {
                    out.println(failure.toString());
                }
                else if (response.isExists()) {
                    String jsonTmp = response.getSourceAsString();
                    out.println(jsonTmp);
                }
                else {
                    out.println("Non exist for " + response.toString());
                }
            }


            //Document APIS bulk api

            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

            bulkRequestBuilder.add(client.prepareIndex("a","b","010")
            .setSource(jsonBuilder()
            .startObject()
                    .field("user","kimchy")
                    .field("postDate",new Date())
                    .field("message","xxxxxxxxxxxxxxx")
            .endObject()));

            bulkRequestBuilder.add(client.prepareIndex("a","c","020")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user","kimchy")
                            .field("postDate",new Date())
                            .field("message","xxxxxxxxxxxxxxx")
                            .endObject()));

            BulkResponse bulkResponse = bulkRequestBuilder.get();
            if (bulkResponse.hasFailures()) {
                out.println("bulk failed as follows:");
                for (BulkItemResponse response : bulkResponse) {
                    if (response.isFailed()) {
                        err.println(response.toXContent(jsonBuilder().prettyPrint(),null).string());
                    }
                }
            }
            else {
                for (BulkItemResponse response : bulkResponse) {
                    if (!response.isFailed()) {
                        out.println(response.toXContent(jsonBuilder().prettyPrint(),null).string());
                    }
                }
            }

            // on shutdown
//            client.close();
        }
        catch (Exception ex) {
            LOG.error(ExceptionUtil.getTrace(ex));
        }
    }
}
