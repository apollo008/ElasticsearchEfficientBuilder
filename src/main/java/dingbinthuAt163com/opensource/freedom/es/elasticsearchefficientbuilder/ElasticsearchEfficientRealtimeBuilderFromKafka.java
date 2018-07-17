package dingbinthuAt163com.opensource.freedom.es.elasticsearchefficientbuilder;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import org.apache.commons.cli.*;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import java.util.concurrent.TimeUnit;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-09, 0:29
 */
public class ElasticsearchEfficientRealtimeBuilderFromKafka {
    private static Logger LOG = LoggerFactory.getLogger(ElasticsearchEfficientRealtimeBuilderFromKafka.class);

    public static final String EXE_NAME  = "ElasticsearchEfficientRealtimeBuilderFromKafka";

    public static final Integer ES_DEFAULT_BULK_ACTIONS = 10000;
    public static final Integer ES_DEFAULT_BULK_SIZE = 10;  //unit MB
    public static final Integer ES_DEFAULT_FLUSH_INTERVAL = 5;  //unit seconds.
    public static final Integer ES_DEFAULT_BULK_CONCURRENT = 3;
    public static final Integer KAFKA_DEFAULT_THREAD_NUM = 3;

    private static Options defineOptions() {
        Options opts = new Options();

        Option op = new Option("h","help",false,"print help information");
        op.setRequired(false); opts.addOption(op);

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

        op = new Option("interval","es-flush-interval",true,"use given number to set elasticsearch bulk flush interval,unit seconds. default " + ES_DEFAULT_FLUSH_INTERVAL);
        op.setRequired(false); opts.addOption(op);


        op = new Option("groupid","kafka-group-id",true,"Kafka group id to consume");
        op.setRequired(true); opts.addOption(op);

        op = new Option("topic","kafka-topic",true,"Kafka topic to consume");
        op.setRequired(true); opts.addOption(op);

        op = new Option("kfkthreads","kafka-threads-num",true,"Kafka thread number to consume,default " + "" + KAFKA_DEFAULT_THREAD_NUM);
        op.setRequired(false); opts.addOption(op);

        op = new Option("lock","kafka-lockfile",true,"lock file path to decide whether bulk index to es");
        op.setRequired(false); opts.addOption(op);

        op = new Option("zkconnect","kafka-zookeeper-connect",true,"Zookeeper ip:host to connect kafka, eg. localhost:2181");
        op.setRequired(true); opts.addOption(op);

        return opts;
    }

    private static String getUsage(Options opts) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(stringWriter);
        HelpFormatter formatter = new HelpFormatter();
        String footer = "\n\nFor example: \n\njava -cp ElasticsearchEfficientBuilder-2.0.jar  dingbinthuAt163com.opensource.freedom.es.elasticsearchefficientbuilder.ElasticsearchEfficientRealtimeBuilderFromKafka -groupid myGroup -topic mytopic -cluster \"es-cluster\" -host 192.168.0.110 -port 9300 -index myindex -type mytype -bconcurrent 4 -bcnt 50000 -bsz 30 -interval 6 -kfkthreads 3 -zkconnect 192.168.0.110:2181,192.168.0.110:12181,192.168.0.110:22181 \n\n" ;
        String declarationStr = "************owned by DingBin,dingbinthu@163.com************";
        footer += declarationStr;
        formatter.printHelp(pw, 130, ElasticsearchEfficientFullBuilderFromMysql.EXE_NAME,null, opts, formatter.getLeftPadding(),formatter.getDescPadding(),footer, true);
        String usage = stringWriter.getBuffer().toString() + "\n\n";
        return usage;
    }

    public static void main(String[] args) {
        Options opts = defineOptions();
        TransportClient esClient = null;
        int exitCode = 0;
        try {
            CommandLineParser cmdlineparser = new GnuParser();
            CommandLine cmdline = cmdlineparser.parse(opts,args);

            if (cmdline.hasOption("h")) {
                // print usage
                LOG.info(getUsage(opts));
                exitCode = 0;
            }
            else {
                long start = System.currentTimeMillis();

                String esHost = cmdline.getOptionValue("host");
                int esPort = Integer.valueOf(cmdline.getOptionValue("port"));
                String clusterName = cmdline.getOptionValue("cluster");
                String esIndex = cmdline.getOptionValue("index");
                String esType = cmdline.getOptionValue("type");

                Integer bulkActions = Integer.valueOf(cmdline.getOptionValue("bcnt","" + ES_DEFAULT_BULK_ACTIONS));
                Integer bulkSize = Integer.valueOf(cmdline.getOptionValue("bsz","" + ES_DEFAULT_BULK_SIZE));
                Integer bulkConCurrent = Integer.valueOf(cmdline.getOptionValue("bconcurrent","" + ES_DEFAULT_BULK_CONCURRENT));
                Integer flushInterval = Integer.valueOf(cmdline.getOptionValue("interval","" + ES_DEFAULT_FLUSH_INTERVAL));

                String groupId  = cmdline.getOptionValue("groupid");
                String topic = cmdline.getOptionValue("topic");
                String lockfilepath = cmdline.getOptionValue("lock");
                String zkConnect = cmdline.getOptionValue("zkconnect");
                Integer kafkaThreadNum = Integer.valueOf(cmdline.getOptionValue("kfkthreads","" + KAFKA_DEFAULT_THREAD_NUM));


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
                        .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                        .setConcurrentRequests(bulkConCurrent)
                        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50),8))
                        .build();

                Kafka2EsConsumer kafka2EsConsumer = new Kafka2EsConsumer(groupId,topic,kafkaThreadNum,zkConnect,lockfilepath);
                kafka2EsConsumer.run(esClient,bulkProcessor,esIndex,esType);

                // Flush any remaining requests
                bulkProcessor.flush();
                // Or close the bulkProcessor if you don't need it anymore
                // bulkProcessor.close();
                if (bulkConCurrent == 0) {
                    bulkProcessor.close();
                }
                else {
                    bulkProcessor.awaitClose(10,TimeUnit.MINUTES);
                }
                // Refresh your indices
                esClient.admin().indices().prepareRefresh().get();

                long timeSecs = (System.currentTimeMillis() - start) /1000;
                long timeMin = timeSecs / 60;
                timeSecs = timeSecs - 60 *  timeMin ;

                System.out.println("time-span: " + timeMin +" 分" +  timeSecs + " 秒.");
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
        catch (Exception ex ) {
            LOG.error(ExceptionUtil.getTrace(ex));
            exitCode = -1;
        }
        finally {
            if (null != esClient) { esClient.close(); esClient = null ; }
            System.exit(exitCode);
        }
    }
}
