package com.aslihankocabas.realtimeprocessing.spark;

import com.aslihankocabas.realtimeprocessing.hbase.HBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

public class SparkConsumer {
    JavaStreamingContext streamingContext;

    public SparkConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "big_data_");
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("quotes");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RealTimeProcesssingApp");
        sparkConf.setMaster("local[*]");
//        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String> Subscribe(topics, props));

        HBase hbase = new HBase();
        JavaDStream<String> stockStream = stream.map(entry -> entry.value());
        stockStream.foreachRDD(rdd -> {
            List<String> quotes = rdd.collect();
            for(String q : quotes) {
                hbase.saveHBase(q);
            }
        });

        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });
    }

    public void consume() {
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
