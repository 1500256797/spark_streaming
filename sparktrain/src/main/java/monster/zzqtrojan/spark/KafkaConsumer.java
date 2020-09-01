package monster.zzqtrojan.spark;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import scala.Int;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName: KafkaConsumer
 * Description: kafka 消费者
 * Author : zzq
 **/
public class KafkaConsumer extends Thread {
    private String topic;


    public KafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnector(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumerConnector = createConnector();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //top对应的数据流
        topicCountMap.put(topic, 1);
        //String : topic
        //List<KafkaStream<byte[], byte[]>> : 对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = createConnector().createMessageStreams(topicCountMap);

        //获取topic下的数据啦
        KafkaStream<byte[], byte[]> stram   = messageStream.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> iterator  = stram.iterator();

        while ((iterator.hasNext())) {
            String message = new String(iterator.next().message());
            System.out.println("rec:" + message);
        }
    }
}
