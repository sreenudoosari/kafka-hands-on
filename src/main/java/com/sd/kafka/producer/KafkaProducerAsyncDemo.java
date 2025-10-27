package com.sd.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerAsyncDemo {

    public static void main(String[] args) {
        // 1. Set producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2. Create Kafka producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String value = "Hello Kafka (async)3";
            ProducerRecord<String, String> record = new ProducerRecord<>("my_topic", "AB", value);

            // 3. Asynchronous send with callback
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception == null) {
                    System.out.println("Message sent asynchronously:");
                    System.out.println("  Topic: " + metadata.topic());
                    System.out.println("  Partition: " + metadata.partition());
                    System.out.println("  Offset: " + metadata.offset());
                } else {
                    System.err.println("Error while sending message:");
                    exception.printStackTrace();
                }
            });

            //producer.send(record, new DemoProducerCallback());

            // Ensure data is sent before closing producer
            //producer.flush();
            System.out.println("Async send initiated (non-blocking).");
        }
    }
}
