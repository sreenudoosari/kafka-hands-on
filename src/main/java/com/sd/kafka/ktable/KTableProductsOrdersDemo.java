package com.sd.kafka.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import java.util.Properties;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

/*
Input data

Topic: products

Key=P1, Value=Phone
Key=P2, Value=TV

Topic: orders
Key=P1, Value=Order#101
Key=P2, Value=Order#102

Output: enriched-orders
Key=P1, Value=Order=Order#101, Product=Phone
Key=P2, Value=Order=Order#102, Product=TV

Concept	Demonstrated
KTable	Keeps latest product per key
KStream	Represents continuous order events
Join	Stream-table join for enrichment
Local state	Products table stored as a StateStore
Output	New topic with enriched results

 */
public class KTableProductsOrdersDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // KTable for product reference data
        KTable<String, String> products = builder.table("products");
        System.out.println("got products");
        // KStream for order events
        KStream<String, String> orders = builder.stream("orders");
        System.out.println("got orders");
        // Join stream with table
        KStream<String, String> enriched = orders.join(
            products,
            (orderValue, productValue) -> "Order=" + orderValue + ", Product=" + productValue
        );

        // Send enriched data to a new topic
        enriched.to("enriched-orders");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("finished");
    }
}