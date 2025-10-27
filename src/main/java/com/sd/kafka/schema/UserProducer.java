package com.sd.kafka.schema_registry;

import com.sd.kafka.avro.User;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class UserProducer {

    public static void main(String[] args) {
        String topic = "users-avro";
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "schema-registry");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // Create example User objects
        User user1 = User.newBuilder()
                .setName("Alice2")
                .setAddress("123 Main Street")
                .setUserId("u0012")
                .setCountry("FR")
                .setPhone("123")
                .build();

        User user2 = User.newBuilder()
                .setName("Bob2")
                .setAddress("456 Second Avenue")
                .setUserId("u0022")
                .setCountry("US")
                .setPhone("456")
                .build();

        try (KafkaProducer<String, User> producer = new KafkaProducer<>(properties)) {
            // Send records
            producer.send(new ProducerRecord<>(topic, user1.getUserId().toString(), user1));
            producer.send(new ProducerRecord<>(topic, user2.getUserId().toString(), user2));
            producer.flush();
            System.out.println("Sent User records to topic " + topic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
