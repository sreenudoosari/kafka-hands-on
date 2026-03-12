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
Step 1: Start your Kafka / Redpanda cluster

Make sure your Kafka or Redpanda cluster is running on:

localhost:19092
Step 2: Create the topics

You need three topics:

products – KTable reference data

orders – KStream events

enriched-orders – output topic

Use the Kafka CLI:

# Products topic
kafka-topics --create --topic products --bootstrap-server localhost:19092 --partitions 1 --replication-factor 1

# Orders topic
kafka-topics --create --topic orders --bootstrap-server localhost:19092 --partitions 1 --replication-factor 1

# Enriched orders topic
kafka-topics --create --topic enriched-orders --bootstrap-server localhost:19092 --partitions 1 --replication-factor 1
Step 3: Produce initial data for the KTable

KTable reflects the latest value per key. Produce initial products:

kafka-console-producer --topic products --bootstrap-server localhost:19092 --property parse.key=true --property key.separator=,

Type:

P1,Phone
P2,TV

Step 4: Produce data for the KStream

Produce some orders events:

kafka-console-producer --topic orders --bootstrap-server localhost:19092 --property parse.key=true --property key.separator=,

Type:

P1,Order#101
P2,Order#102

Step 6: Run the Kafka Streams application

Compile and run:

mvn compile exec:java -Dexec.mainClass="com.sd.kafka.ktable.KTableProductsOrdersDemo"

Or run directly from your IDE.

The app will load products as a KTable.

orders as a KStream.

Join them to create enriched-orders.

Print the KTable state via toStream().foreach().

Step 7: Verify enriched output

Consume enriched-orders:

kafka-console-consumer --topic enriched-orders --bootstrap-server localhost:19092 --from-beginning --property print.key=true --property key.separator=,

Expected output:

P1,Order=Order#101, Product=Phone
P2,Order=Order#102, Product=TV
Step 8: Demonstrate KTable updating only latest values

Produce a product update:

P1,Smartphone

Produce a new order:

P1,Order#103

Observe:

Console prints the updated KTable state:

KTable current state: P1 -> Smartphone

Output in enriched-orders:

P1,Order=Order#103, Product=Smartphone

Key concept: The KTable always holds the latest value per key, even if multiple updates happen.




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
        // Print current KTable values whenever they change
        products.toStream()
                .foreach((key, value) ->
                        System.out.println("KTable current state: " + key + " -> " + value)
                );
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