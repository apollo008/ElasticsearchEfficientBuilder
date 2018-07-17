package dingbinthuAt163com.opensource.freedom.es.elasticsearchefficientbuilder;

import dingbinthuAt163com.opensource.freedom.es.jdbc.JdbcUtils;
import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.apache.commons.cli.*;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-09, 0:29
 */
public class ElasticsearchEfficientFullBuilderFromMysql {
    private static Logger LOG = LoggerFactory.getLogger(ElasticsearchEfficientFullBuilderFromMysql.class);
    public static final String EXE_NAME  = "ElasticsearchEfficientFullBuilderFromMysql";
    public static final String MYSQL_DRIVER_NAME =  "com.mysql.jdbc.Driver";
    public static final Integer MYSQL_DEFAULT_PAGE_SIZE =  10000;
    public static final Integer MYSQL_DEFAULT_START = 0;
    public static final Integer MYSQL_DEFAULT_END = 2000000000 ;
    public static final Integer ES_DEFAULT_BULK_ACTIONS = 10000;
    public static final Integer ES_DEFAULT_BULK_SIZE = 10;  //unit MB
    public static final Integer ES_DEFAULT_BULK_CONCURRENT = 0;
    public static final Integer ES_DEFAULT_BULK_AWAIT = 60; //unit minutes

    private static Options defineOptions() {
        Options opts = new Options();

        Option op = new Option("h","help",false,"print help information");
        op.setRequired(false); opts.addOption(op);

        op = new Option("url","mysql-url",true,"use given mysql-url from import data to es");
        op.setRequired(true); opts.addOption(op);

        op = new Option("u","mysql-user",true,"use given mysql-user to connect mysql database");
        op.setRequired(true); opts.addOption(op);

        op = new Option("p","mysql-password",true,"use given mysql-password to connect mysql database");
        op.setRequired(true); opts.addOption(op);

        op = new Option("sql","mysql-sql",true,"use given mysql sql sentence to extract data from mysql");
        op.setRequired(true); opts.addOption(op);

        op = new Option("host","es-host",true,"use given elasticsearch host to connect es cluster");
        op.setRequired(true); opts.addOption(op);

        op = new Option("port","es-port",true,"use given elasticsearch port to connect es cluster");
        op.setRequired(true); opts.addOption(op);

        op = new Option("cluster","es-cluster-name",true,"use given elasticsearch cluster name to discover specified es cluster");
        op.setRequired(true); opts.addOption(op);


        op = new Option("index","es-index",true,"use given elasticsearch index name to specify es index");
        op.setRequired(true); opts.addOption(op);

        op = new Option("type","es-type",true,"use given elasticsearch type name to specify es type");
        op.setRequired(true); opts.addOption(op);


        op = new Option("bcnt","es-bulk-count",true,"use given number to set elasticsearch bulkActions,default " + ES_DEFAULT_BULK_ACTIONS);
        op.setRequired(false); opts.addOption(op);

        op = new Option("bsz","es-bulk-size",true,"use given number to set elasticsearch bulkSize,unit MB,default " + ES_DEFAULT_BULK_SIZE +" (MB)");
        op.setRequired(false); opts.addOption(op);

        op = new Option("bconcurrent","es-bulk-concurrent-thread-size",true,"use given number to set elasticsearch concurrent threads number,0 indicates synchronous. default " + ES_DEFAULT_BULK_CONCURRENT);
        op.setRequired(false); opts.addOption(op);

        op = new Option("bawait","es-bulk-awit-time",true,"use given number to set elasticsearch await time close bulk processor,unit minutes. default " + ES_DEFAULT_BULK_AWAIT);
        op.setRequired(false); opts.addOption(op);


        op = new Option("start","mysql-start-offset",true,"use given number to set from offset used in sql select,default " + MYSQL_DEFAULT_START);
        op.setRequired(false); opts.addOption(op);

        op = new Option("end","mysql-end-offset",true,"use given number to set end offset used in sql select,default " + MYSQL_DEFAULT_END);
        op.setRequired(false); opts.addOption(op);

        op = new Option("pagesz","mysql-page-size",true,"use given number to set page size when sql select,default " + MYSQL_DEFAULT_PAGE_SIZE);
        op.setRequired(false); opts.addOption(op);


        return opts;
    }

    private static String getUsage(Options opts) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(stringWriter);
        HelpFormatter formatter = new HelpFormatter();
        String footer = "\n\nFor example: \n\njava -jar ElasticsearchEfficientBuilder-2.0.jar -url \"jdbc:mysql://192.168.0.101:3306?useUnicode=true&characterEncoding=UTF-8\"  -u root -p 123456  -sql \"select name,alias, address from db.table\"  -host 192.168.0.110 -port 9300 -cluster \"es-cluster\" -index testmysql -type community -bconcurrent 6 -bcnt 50000 -bsz 50 -start 100 -end 10000 -pagesz 100000\n\n" ;
        String declarationStr = "************owned by DingBin,dingbinthu@163.com************";
        footer += declarationStr;
        formatter.printHelp(pw, 130, ElasticsearchEfficientFullBuilderFromMysql.EXE_NAME,null, opts, formatter.getLeftPadding(),formatter.getDescPadding(),footer, true);
        String usage = stringWriter.getBuffer().toString() + "\n\n";
        return usage;
    }

    public static void main(String[] args) {
        Options opts = defineOptions();
        Connection conn = null;
        TransportClient esClient = null;
        int exitCode = 0;
        try {
            CommandLineParser cmdlineparser = new GnuParser();
            CommandLine  cmdline = cmdlineparser.parse(opts,args);

            if (cmdline.hasOption("h")) {
                // print usage
                LOG.info(getUsage(opts));
                exitCode = 0;
            }
            else {
                long start = System.currentTimeMillis();
                String url = cmdline.getOptionValue("url");
                String user = cmdline.getOptionValue("u");
                String passwd = cmdline.getOptionValue("p");
                String sql = cmdline.getOptionValue("sql");

                Integer stOff = Integer.valueOf(cmdline.getOptionValue("start","" + MYSQL_DEFAULT_START));
                Integer edOff = Integer.valueOf(cmdline.getOptionValue("end","" + MYSQL_DEFAULT_END));
                Integer pageSz  = Integer.valueOf(cmdline.getOptionValue("pagesz","" + MYSQL_DEFAULT_PAGE_SIZE));
                if (stOff > edOff ) {
                    throw new Exception("ERROR: 'start' option value '" + stOff +"' can not be larger than the end option value '" + edOff + "'.");
                }

                String esHost = cmdline.getOptionValue("host");
                int esPort = Integer.valueOf(cmdline.getOptionValue("port"));
                String clusterName = cmdline.getOptionValue("cluster");
                String esIndex = cmdline.getOptionValue("index");
                String esType = cmdline.getOptionValue("type");

                Integer bulkActions = Integer.valueOf(cmdline.getOptionValue("bcnt","" + ES_DEFAULT_BULK_ACTIONS));
                Integer bulkSize = Integer.valueOf(cmdline.getOptionValue("bsz","" + ES_DEFAULT_BULK_SIZE));
                Integer bulkConCurrent = Integer.valueOf(cmdline.getOptionValue("bconcurrent","" + ES_DEFAULT_BULK_CONCURRENT));
                Integer bulkAwait = Integer.valueOf(cmdline.getOptionValue("bawait","" + ES_DEFAULT_BULK_AWAIT));




                Class.forName(ElasticsearchEfficientFullBuilderFromMysql.MYSQL_DRIVER_NAME);
                conn = DriverManager.getConnection(url,user,passwd);



                final Settings settings = Settings.builder()
                        .put("cluster.name", clusterName)
                        .put("client.transport.sniff", true)
                        .put("client.transport.ping_timeout", 120, TimeUnit.SECONDS)
                        .put("client.transport.nodes_sampler_interval",5, TimeUnit.SECONDS)
                        .build();

                esClient = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), esPort));


                BulkProcessor bulkProcessor = BulkProcessor.builder(esClient, new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        LOG.info("executionId:" + executionId + ",request number:" + request.numberOfActions() + ",request size:"
                                + request.estimatedSizeInBytes()  + " Bytes.");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        if (response.hasFailures()) {
                            LOG.error("executionId:" + executionId + " has failures:\n" + response.buildFailureMessage());
                        }
//                        else {
//                            Iterator<BulkItemResponse> iter = response.iterator();
//                            while(iter.hasNext()) {
//                                BulkItemResponse item = iter.next();
//                                try {
//                                    LOG.info(item.toXContent(jsonBuilder().prettyPrint(),null).string());
//                                }
//                                catch (IOException ex) {
//                                    ex.printStackTrace();
//                                }
//                            }
//                        }
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        LOG.error("executionId:" + executionId + ", throwable:" + ExceptionUtil.getTrace(failure));
                    }
                }).setBulkActions(bulkActions)
                  .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
//                .setFlushInterval(TimeValue.timeValueSeconds(5))
                  .setConcurrentRequests(bulkConCurrent)
                  .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50),8))
                  .build();


                sql += " limit ?,? ";
                int pageFrom = (int)Math.max(0l,stOff);
                int pageSize = (int)Math.min(pageSz,edOff-stOff + 1);

                List<Object> params = new ArrayList<>();
                params.add(pageFrom);
                params.add(pageSize);

                List<Map<String, Object>>  resultList = JdbcUtils.findModeResult(conn,sql,params);
                long count = resultList.size();
                while(null != resultList && !resultList.isEmpty()) {
                    for(Map m : resultList) {
                        bulkProcessor.add(new IndexRequest(esIndex,esType).source(m));
                    }
                    pageFrom += pageSize;
                    params.set(0,pageFrom);
                    if (pageFrom > edOff) {
                        break;
                    }
                    else {
                        long diff =  edOff - pageFrom + 1;
                        if ( diff < pageSize) {
                            pageSize = (int)diff;
                            params.set(1,pageSize);
                        }
                    }
                    resultList = JdbcUtils.findModeResult(conn,sql,params);
                    count += resultList.size();
                }


                // Flush any remaining requests
                bulkProcessor.flush();

                // Or close the bulkProcessor if you don't need it anymore
                // bulkProcessor.close();
                if (bulkConCurrent == 0) {
                    bulkProcessor.close();
                }
                else {
                    bulkProcessor.awaitClose(bulkAwait,TimeUnit.MINUTES);
                }

                // Refresh your indices
                esClient.admin().indices().prepareRefresh().get();

                long timeSecs = (System.currentTimeMillis() - start) /1000;
                long timeMin = timeSecs / 60;
                timeSecs = timeSecs - 60 *  timeMin ;

                System.out.println("Records built totally: " + count + " ,time-span: " + timeMin +" 分" +  timeSecs + " 秒.");
                exitCode = 0;
            }
        }
        catch (ParseException paex) {
            if (null != args && args.length == 1 && ("-h".equals(args[0]) || "--help".equals(args[0])) ) {
                // print usage
                LOG.info(getUsage(opts));
                exitCode = 0;
            }
            else {
                LOG.error(ExceptionUtil.getTrace(paex));

                // print usage
                LOG.info(getUsage(opts));
                exitCode = -1;
            }
        }
        catch (SQLException sqlex ) {
            LOG.error(ExceptionUtil.getTrace(sqlex));
            exitCode = -1;
        }
        catch (Exception ex ) {
            LOG.error(ExceptionUtil.getTrace(ex));
            exitCode = -1;
        }
        finally {
            if (null != conn) { try { conn.close(); } catch (SQLException tmpslqex) { LOG.error(ExceptionUtil.getTrace(tmpslqex)); } }
            if (null != esClient) { esClient.close(); esClient = null ; }
            System.exit(exitCode);
        }
    }

}
