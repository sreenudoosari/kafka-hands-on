package com.sd.kafka.producer.json;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class JsonProducerExample {

    public static void main(String[] args) {
        String topic = "customerContacts";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class);

        try (Producer<String, Customer> producer = new KafkaProducer<>(props)) {
            Customer customer = CustomerGenerator.getNext();
            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, customer.getId(), customer);
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Sent to topic: " + metadata.topic() + ", partition: " + metadata.partition() + ", value: " + record);
            System.out.println("Sent: " + customer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}