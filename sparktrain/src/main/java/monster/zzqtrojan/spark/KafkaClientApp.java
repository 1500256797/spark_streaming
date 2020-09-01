package monster.zzqtrojan.spark;

/**
 * ClassName: KafkaClientApp
 * Description: kafka测试
 * Author : Administrator
 **/
public class KafkaClientApp {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();


    }
}
