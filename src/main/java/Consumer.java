import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String topicName = "TestTopic";

        Properties configuration = new Properties();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata1");
        configuration.put(ConsumerConfig.CLIENT_ID_CONFIG, "finalproject1");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configuration);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
