import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String topicName = "TestTopic";

        Properties configuration = new Properties();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //stringler serialize yapılıyor
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    }
}
