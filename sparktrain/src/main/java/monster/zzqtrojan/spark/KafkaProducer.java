package monster.zzqtrojan.spark;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * ClassName: KafkaProducer
 * Description: kafka生产者
 * Author : zzq
 **/
public class KafkaProducer extends Thread {

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic){
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);//设置broker_list
        properties.put("serializer.class", "kafka.serializer.StringEncoder");//序列号
        properties.put("request.required.acks", "1");//消息确认机制

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "message" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("send:" + message);
            messageNo++;
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
