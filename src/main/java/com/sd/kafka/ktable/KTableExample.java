package com.sd.kafka.ktable;

import com.sd.kafka.streams.basic.Widget;
import com.sd.kafka.streams.basic.WidgetSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KTableExample {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_topic-ktable");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, WidgetSerde.class);
       // props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // frequent commits for demo

        var stringSerde = Serdes.String();
        var widgetSerde = new WidgetSerde();
        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = "my_topic";
        String outputTopic = "my_topic_latest";
        // Build KTable from input topic
        KTable<String, String> firstKTable = builder.table(
                inputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde)
        );

        // Write the KTable’s changelog (latest value per key) into an output topic
        firstKTable.toStream().to(outputTopic, Produced.with(stringSerde, stringSerde));


        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        //System.exit(0);
/*
        // Build KTable from input topic
        KTable<String, Widget> firstKTable = builder.table(
                inputTopic,
                Materialized.<String, Widget, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                        .withKeySerde(stringSerde)
                        .withValueSerde(widgetSerde)
        );

        // Write the KTable’s changelog (latest value per key) into an output topic
        firstKTable.toStream().to(outputTopic, Produced.with(stringSerde, widgetSerde));


        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);

 */
    }
}
