package dingbinthuAt163com.opensource.freedom.es.elasticsearchefficientbuilder;

import dingbinthuAt163com.opensource.freedom.es.util.ExceptionUtil;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;


/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-09, 0:29
 */
public class Kafka2EsWorker implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(Kafka2EsWorker.class);
    private SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");

    private          CountDownLatch     countDownLatch;

    @SuppressWarnings("rawtypes")
    private          KafkaStream         kafkaStream;

    private          int                 threadId = 0;
    private          String              lockfilepath = null;
    private          File                lockFile     = null;


    private          TransportClient     esClient;
    private          BulkProcessor       bulkProcessor;
    private          String              indexName;
    private          String              typeName;

    @SuppressWarnings("rawtypes")
    public Kafka2EsWorker(CountDownLatch countDownLatch, KafkaStream kafkaStream,
                          int threadId, String lockfilepath,
                          TransportClient esClient, BulkProcessor bulkProcessor, String indexName, String typeName) {
        this.countDownLatch = countDownLatch;
        this.kafkaStream = kafkaStream;
        this.threadId = threadId;
        this.lockfilepath = lockfilepath;
        this.esClient = esClient;
        this.bulkProcessor = bulkProcessor;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    private Boolean isLock() {
        if (StringUtils.isBlank(lockfilepath)) {
            return false;
        }
        if (null == this.lockFile) {
            this.lockFile = new File(lockfilepath);
        }
        return lockFile.exists();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        int tick = 0;
        while (it.hasNext()) {
            try {
                MessageAndMetadata<byte[], byte[]> thisMetadata = it.next();
                String message = new String(thisMetadata.message(), "utf-8");
//                LOG.info("======= partion:"+thisMetadata.partition()+",offset:"+thisMetadata.offset() + "threadId:" + threadId + ",content:" + message);

                while (isLock()) {
                    if (tick == 0) { LOG.warn(sdf.format(new Date()) + ":Waiting for bulk processor on index:" + indexName + ",type:" + typeName + ",as lock-file:" + lockfilepath + " exists......"); }
                    if (++tick >= 60) { tick = 0; }
                    try { Thread.sleep(3 * 1000); } catch (java.lang.InterruptedException iex) { LOG.error(ExceptionUtil.getTrace(iex)); }
                }

                //elasticsearch bulk processor
                bulkProcessor.add(new IndexRequest(indexName, typeName).source(message, XContentType.JSON));

            }
            catch (Exception ex) {
                LOG.error(ExceptionUtil.getTrace(ex));
            }
        }
        countDownLatch.countDown();
    }
}
