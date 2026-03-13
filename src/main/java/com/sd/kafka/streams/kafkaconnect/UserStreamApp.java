package com.sd.kafka.streams.kafkaconnect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;

public class UserStreamApp {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pg-user-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("pg-users");
        KStream<String, PGUser> users = source.mapValues(value -> {
            try {
                JsonNode root = mapper.readTree(value);
                JsonNode payload = root.get("payload");
                return mapper.treeToValue(payload, PGUser.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).filter((k, v) -> v != null)
        .mapValues(user -> new PGUser(user.id(), user.name().toUpperCase(), user.email()));
        users.to("pg-uppercase-users",
                Produced.with(Serdes.String(), new JsonSerde<>(PGUser.class)));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}