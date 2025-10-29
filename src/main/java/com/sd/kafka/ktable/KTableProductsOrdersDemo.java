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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class KTableProductsOrdersDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // KTable for product reference data
        KTable<String, String> products = builder.table("products");

        // KStream for order events
        KStream<String, String> orders = builder.stream("orders");

        // Join stream with table
        KStream<String, String> enriched = orders.join(
            products,
            (orderValue, productValue) -> "Order=" + orderValue + ", Product=" + productValue
        );

        // Send enriched data to a new topic
        enriched.to("enriched-orders");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}