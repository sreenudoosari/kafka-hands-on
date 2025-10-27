package com.sd.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerFireAndForgetDemo {


    public static void main(String[] args) throws Exception {
        // 1. Set config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2. Create producer
        // Try-with-resources for auto-closing
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String value = "Hello Kafka fire and forget.2";
            ProducerRecord<String, String> record = new ProducerRecord<>("my_topic","AB", value);
            // 3. Fire-and-forget send — no get(), no callback
            producer.send(record);
            // Optional: flush to force delivery before closing
            //producer.flush();
            System.out.println("Message sent (fire and forget). No acknowledgment awaited.");
        }
    }
}
