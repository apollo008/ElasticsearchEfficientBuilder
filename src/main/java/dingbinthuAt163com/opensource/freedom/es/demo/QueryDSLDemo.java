package dingbinthuAt163com.opensource.freedom.es.demo;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import com.vividsolutions.jts.geom.Coordinate;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 17:11
 */
public class QueryDSLDemo {
    private static Logger LOG = LoggerFactory.getLogger(QueryDSLDemo.class);

    public static void main(String[] args)  {
        try {
            TransportClient client = EsClient.getClient();

            QueryBuilder maqb = QueryBuilders.matchAllQuery();
            SearchResponse searchResponse = client.prepareSearch().setQuery(maqb).setFrom(0).setSize(10).get();
            out.println(searchResponse.toXContent(jsonBuilder().prettyPrint(),null).string());

            QueryBuilder mqb = QueryBuilders.matchQuery("name","xxxxx");
            QueryBuilder mmqb = QueryBuilders.multiMatchQuery("texttextadrasfasdf","name","title","content");
            QueryBuilder ctqb = QueryBuilders.commonTermsQuery("name","xxxxx");
            QueryBuilder qsqb = QueryBuilders.queryStringQuery("+kimchy -elasticsearch");
            QueryBuilder sqsqb = QueryBuilders.simpleQueryStringQuery("+kimchy -elasticsearch");

            QueryBuilder tqb = QueryBuilders.termQuery("name","xxxx");
            QueryBuilder tsqb = QueryBuilders.termsQuery("name","jim","tom","jerry");

            QueryBuilder rqb = QueryBuilders.rangeQuery("price")
                    .from(5)
                    .to(10)
                    .includeLower(true)
                    .includeUpper(false);


            QueryBuilder eqb = QueryBuilders.existsQuery("name");

            QueryBuilder pqb = QueryBuilders.prefixQuery("name","a");

            QueryBuilder wqb = QueryBuilders.wildcardQuery("user","k?mc*");

            QueryBuilder reqb = QueryBuilders.regexpQuery("name.first","s.*y");


            QueryBuilder csqb = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name","xxxx")).boost(2.0f);

            QueryBuilder bqb = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("content","test1"))
                    .must(QueryBuilders.termQuery("content","test4"))
                    .mustNot(QueryBuilders.termQuery("content","test2"))
                    .should(QueryBuilders.termQuery("content","test3"))
                    .filter(QueryBuilders.termQuery("content","test5"));


            QueryBuilder dmqb = QueryBuilders.disMaxQuery()
                    .add(QueryBuilders.termQuery("name","kimchy"))
                    .add(QueryBuilders.termQuery("name","elasticsearch"))
                    .boost(1.2f)
                    .tieBreaker(0.7f);

            QueryBuilder iqb1 = QueryBuilders.indicesQuery(QueryBuilders.termQuery("tag","wow"),"index1","index2")
                    .noMatchQuery("all");

            QueryBuilder iqb2 = QueryBuilders.indicesQuery(QueryBuilders.termQuery("tag","wow"),"index1","index2")
                    .noMatchQuery(QueryBuilders.termQuery("tag","kow"));


            List<Coordinate> points = new ArrayList<>();
            points.add(new Coordinate(0,0));
            points.add(new Coordinate(0,10));
            points.add(new Coordinate(10,10));
            points.add(new Coordinate(10,0));
            points.add(new Coordinate(0,0));


            QueryBuilder gsqb = QueryBuilders.geoShapeQuery("pin.location", ShapeBuilders.newMultiPoint(points))
                    .relation(ShapeRelation.WITHIN);

            QueryBuilder gbqb = QueryBuilders.geoBoundingBoxQuery("pin.location")
                    .setCorners(40.73,-74.1,40.717,-73.99);

            QueryBuilder gdqb = QueryBuilders.geoDistanceQuery("pin.location")
                    .point(40,70)
                    .distance(200, DistanceUnit.KILOMETERS);

            List<GeoPoint> points2 = new ArrayList<>();
            points2.add(new GeoPoint(0,10));
            points2.add(new GeoPoint(1000,2000));
            points2.add(new GeoPoint(3000,4000));

            QueryBuilder gpqb = QueryBuilders.geoPolygonQuery("pin.location",points2);

        }
        catch (Exception ex) {
            LOG.error(ExceptionUtil.getTrace(ex));
        }
    }
}
