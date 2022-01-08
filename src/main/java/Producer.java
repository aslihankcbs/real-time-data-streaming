import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String topicName = "TestTopic";

        Properties configuration = new Properties();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(configuration);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "producertest");
        producer.send(record);
        producer.close();
    }
}
