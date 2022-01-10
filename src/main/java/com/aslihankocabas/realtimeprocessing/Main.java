package com.aslihankocabas.realtimeprocessing;

import com.aslihankocabas.realtimeprocessing.kafka.DataProducer;
import com.aslihankocabas.realtimeprocessing.spark.SparkConsumer;

public class Main {
    public static void main(String[] args) {
        DataProducer producer = new DataProducer();
        producer.start();

        SparkConsumer sparkConsumer = new SparkConsumer();
        sparkConsumer.consume();
    }
}
