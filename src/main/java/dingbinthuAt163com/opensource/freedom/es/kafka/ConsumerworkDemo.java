package dingbinthuAt163com.opensource.freedom.es.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;


/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-09, 0:29
 */
public class ConsumerworkDemo implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(ConsumerworkDemo.class);
    private CountDownLatch countDownLatch;

    @SuppressWarnings("rawtypes")
    private KafkaStream m_stream;

    private int m_threadNumber;


    @SuppressWarnings("rawtypes")
    public ConsumerworkDemo(CountDownLatch a_countDownLatch, KafkaStream a_stream, int a_threadNumber) {
        countDownLatch = a_countDownLatch;
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        // TODO Auto-generated method stub
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            try {
                MessageAndMetadata<byte[], byte[]> thisMetadata=it.next();
                String jsonStr = new String(thisMetadata.message(),"utf-8") ;
                LOG.info("Thread " + m_threadNumber + ": " +jsonStr);
                LOG.info("partion"+thisMetadata.partition()+",offset:"+thisMetadata.offset());
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        countDownLatch.countDown();
    }
}
