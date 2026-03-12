package com.sd.kafka.streams.basic;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UppercaseWithFilterStreamsApp {


    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "uppercase-names-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> namesStream =
                builder.stream("names", Consumed.with(Serdes.String(), Serdes.String()));

        namesStream
                .filter((key, name) -> name != null && name.toLowerCase().contains("vlad"))
                .mapValues(name -> name.toUpperCase())
                .to("UPPER_names_with_vlad", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        streams.start();

        // Block the main thread so the app keeps running
        latch.await();
    }
}