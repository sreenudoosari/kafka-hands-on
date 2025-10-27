package com.sd.kafka.schema_registry;

import com.sd.kafka.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class UserConsumer {

    public static void main(String[] args) {
        String topic = "users-avro";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Tell deserializer to return SpecificRecord (User) instead of GenericRecord
        properties.put("specific.avro.reader", true);

        try (KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Consuming from topic: " + topic);

            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, User> record : records) {
                    User user = record.value();
                    System.out.printf("Consumed record key=%s value=%s%n",
                            record.key(), user);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
