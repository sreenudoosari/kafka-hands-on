package com.sd.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerSyncDemo {


    public static void main(String[] args) throws Exception {
        // 1. Set config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2. Create producer
        String topic = "test_topic";
        // Try-with-resources for auto-closing
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String value = "Hello Kafka (sync) "+ UUID.randomUUID();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,"def", value);
            RecordMetadata metadata = producer.send(record).get(); // sync send
            System.out.println("Sent to topic: " + metadata.topic() + ", partition: " + metadata.partition() + ", value: " + value);
        }
    }
}
