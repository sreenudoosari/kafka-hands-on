package com.sd.kafka.producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class TopicLoader {

    public static final String FIRST_TOPIC = "test_topic";
    public static void main(String[] args) throws IOException {
        createTopics();
    }

    public static void createTopics() throws IOException {

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try(Admin adminClient = Admin.create(properties)) {
            NewTopic firstTopic = new NewTopic(FIRST_TOPIC, 5, (short) 2);
            adminClient.createTopics(List.of(firstTopic));
        }
    }
}