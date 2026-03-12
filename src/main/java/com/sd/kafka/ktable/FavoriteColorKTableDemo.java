package com.sd.kafka.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class FavoriteColorKTableDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> colorTable =  builder.table("user-colors");
        colorTable.toStream()
                .foreach((key, value) ->
                        System.out.println("User: " + key + " Color: " + value)
                );
        KafkaStreams streams =  new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook( new Thread(streams::close) );
    }
}