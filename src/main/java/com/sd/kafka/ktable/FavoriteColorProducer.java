package com.sd.kafka.ktable;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.List;
import java.util.Map;

public class FavoriteColorProducer {

    public static void main(String[] args) {

        String topic = "user-colors";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // List of user-color updates
        List<Map.Entry<String, String>> updates = List.of(
                Map.entry("alice", "blue"),
                Map.entry("bob", "green"),
                Map.entry("alice", "red"),
                Map.entry("bob", "yellow"),
                Map.entry("charlie", "purple")
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (Map.Entry<String, String> update : updates) {
                String user = update.getKey();
                String color = update.getValue();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, user, color);
                producer.send(record );
            }
            producer.flush();
        }
    }
}