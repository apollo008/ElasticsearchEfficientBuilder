package dingbinthuAt163com.opensource.freedom.es.kafka;

import dingbinthuAt163com.opensource.freedom.es.util.PathUtils;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.lang.StringUtils;
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
public class KafkaConsumerDemo {
    private static Logger LOG = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    private final String topic;
    private final ConsumerConnector consumer;
    private ExecutorService executor;

    private ConsumerConfig createConsumerConfig(String groupId) throws Exception {
        // TODO Auto-generated method stub
        //加载comsumer的配置文件
        Properties props = getProperties();
        props.put("group.id", groupId);
        return new ConsumerConfig(props);
    }

    public KafkaConsumerDemo(String groupId, String topic) throws Exception {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(groupId));
        this.topic = topic;
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

    private void run(int numThreads) throws InterruptedException {
        // TODO Auto-generated method stub
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        executor = Executors.newCachedThreadPool();

        CountDownLatch countDownLatch = new CountDownLatch(numThreads);
        List<Future> futures = new ArrayList<Future>(numThreads);
        // now create an object to consume the messages
        int threadNumber = 0;
        LOG.info("the streams size is "+ streams.size());
        for (final KafkaStream stream : streams) {
            futures.add(executor.submit(new ConsumerworkDemo(countDownLatch,stream, threadNumber)));
            // consumer.commitOffsets();手动提交offset,当enable.auto.commit=false时使用
            threadNumber++;
        }
        countDownLatch.await();
    }

    public static void main(String [] args) throws  Exception {
        String topic = "testTopic";
        String group = args[0];
        int  numThreads = Integer.valueOf(args[1]);

        KafkaConsumerDemo consumerGroup= new KafkaConsumerDemo(group,topic);
        consumerGroup.run(numThreads);
//        consumerGroup.shutdown();
    }
}
