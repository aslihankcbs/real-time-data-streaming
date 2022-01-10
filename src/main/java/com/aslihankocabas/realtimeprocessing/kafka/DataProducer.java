package com.aslihankocabas.realtimeprocessing.kafka;

import com.aslihankocabas.realtimeprocessing.stockapi.FinnhubApi;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class DataProducer extends Thread {

    Producer<String, String> producer;
    static final Set<String> SYMBOLS = new HashSet<>(Arrays.asList("AAPL", "AMZN", "MSFT"));
    Map<String, String> previousQuotesMap = new HashMap<>();

    public DataProducer() {
        Properties configuration = new Properties();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.RETRIES_CONFIG, 0);
        configuration.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configuration.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configuration.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(configuration);
    }

    @Override
    public void run() {
        produce();
    }

    private void produce() {
        while (true) {
            String newQuotes = getNewQuotes();
            if (!newQuotes.isEmpty()) {
                producer.send(new ProducerRecord<>("quotes", String.valueOf(System.currentTimeMillis()), newQuotes));
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                producer.close();
                e.printStackTrace();
            }
        }
    }

    private String getNewQuotes() {
        StringBuilder quotesBuilder = new StringBuilder();

        for (String symbol : SYMBOLS) {
            String symbolQuote = "";
            try {
                symbolQuote = FinnhubApi.getQuote(symbol);
                if (symbolQuote.isEmpty())
                    continue;
            } catch (Exception e) {
                e.printStackTrace();
            }
            String previousSymbolQuote = previousQuotesMap.get(symbol);
            if (previousSymbolQuote == null || !symbolQuote.equals(previousSymbolQuote)) {
                quotesBuilder.append(symbolQuote);
                previousQuotesMap.put(symbol, symbolQuote);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (quotesBuilder.length() > 0)
            return quotesBuilder.substring(0, quotesBuilder.length() - 1);

        return "";
    }

    @Override
    protected void finalize() throws Throwable {
        producer.close();
        super.finalize();
    }
}
