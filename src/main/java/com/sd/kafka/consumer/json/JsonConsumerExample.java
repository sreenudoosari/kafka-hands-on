package com.sd.kafka.consumer.json;

import com.sd.kafka.producer.json.Customer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class JsonConsumerExample {

    public static void main(String[] args) {
        String topic = "customerContacts";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-consumer-group");

        //AUTO_OFFSET_RESET_CONFIG = "earliest" only takes effect when there’s no committed offset for that consumer group.
        //If your group has already committed offsets, Kafka resumes from the last committed position — even if you restart the consumer.
        //To make the consumer always read from the beginning after every restart, uncomment the line below
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-consumer-group-" + System.currentTimeMillis());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  JsonPOJODeserializer.class);

        // Let the deserializer know what class to map to
        props.put("json.deserializer.target.class", "com.sd.kafka.producer.json.Customer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Listening to topic: " + topic);

            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Customer> record : records) {
                    Customer customer = record.value();
                    System.out.printf("Consumed record with key=%s value=%s partition=%d offset=%d%n",
                            record.key(), customer, record.partition(), record.offset());
                }
            }
        }
    }
}
