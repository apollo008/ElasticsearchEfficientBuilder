package dingbinthuAt163com.opensource.freedom.es.elasticsearchefficientbuilder;

import dingbinthuAt163com.opensource.freedom.es.util.PathUtils;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-09, 0:29
 */

public class Kafka2EsConsumer {
    private static Logger LOG = LoggerFactory.getLogger(Kafka2EsConsumer.class);

    private  String             groupid;
    private  String             topic;
    private  Integer            threadNum;
    private  String             zkConnect;
    private  String             lockfilepath = null;

    private ConsumerConnector   consumer;
    private ExecutorService     executor;

    public Kafka2EsConsumer(String groupId, String topic, Integer threadNum, String zkConnect,String lockfilepath) throws Exception {
        this(groupId,topic,threadNum,zkConnect);
        this.lockfilepath = lockfilepath;
    }

    public Kafka2EsConsumer(String groupId, String topic, Integer threadNum, String zkConnect) throws Exception {

        if (org.apache.commons.lang3.StringUtils.isBlank(groupId)) {
            throw  new Exception("ERROR: invalid kafka parameter for group.id:[" + groupId + "].");
        }
        if (org.apache.commons.lang3.StringUtils.isBlank(topic)) {
            throw  new Exception("ERROR: invalid kafka parameter for topic:[" + topic+ "].");
        }

        if (org.apache.commons.lang3.StringUtils.isBlank(zkConnect)) {
            throw  new Exception("ERROR: invalid kafka parameter for zookeeper.connect:[" + zkConnect+ "].");
        }


        this.groupid = groupId;
        this.topic = topic;
        this.threadNum = threadNum;
        this.zkConnect = zkConnect;

        if (null == threadNum || threadNum < 1) {
            this.threadNum = 1;
        }

        //加载comsumer的配置文件
        Properties props = getProperties();
        props.put("group.id", groupId);
        props.put("zookeeper.connect", zkConnect);
        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public static  Properties getProperties() throws Exception {
        String configFileName = "kafka.comsumer.properties";
        InputStream is = PathUtils.class.getClassLoader().getResourceAsStream(configFileName);
        if (null == is) {
            String tmpPath = PathUtils.getProjRoleFilePath(PathUtils.class,configFileName);
            if (!StringUtils.isBlank(tmpPath)) {
                is = new FileInputStream(new File(tmpPath));
            }
        }
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }

    private void shutdown() {
        // TODO Auto-generated method stub
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOG.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            LOG.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(TransportClient esClient, BulkProcessor bulkProcessor, String indexName, String typeName) throws InterruptedException {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(this.threadNum));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        executor = Executors.newCachedThreadPool();

        CountDownLatch countDownLatch = new CountDownLatch(this.threadNum);
        List<Future> futures = new ArrayList<Future>(this.threadNum);
        // now create an object to consume the messages
        int threadId = 0;
        LOG.info("-----Kafka Consumer threadnum :"+ streams.size());
        for (final KafkaStream stream : streams) {
            futures.add(executor.submit(new Kafka2EsWorker(countDownLatch,stream, ++threadId,this.lockfilepath,esClient,bulkProcessor,indexName,typeName)));
            // consumer.commitOffsets();手动提交offset,当enable.auto.commit=false时使用
        }
        countDownLatch.await();
    }
}
