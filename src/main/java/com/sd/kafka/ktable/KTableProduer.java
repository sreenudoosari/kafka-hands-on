package com.sd.kafka.ktable;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KTableProduer {

    public static void main(String[] args) throws IOException {
        runProducer();
    }


    public static void runProducer() throws IOException {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_topic_ktable");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try(
            Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String inputTopic = "my_topic";

            Callback callback = (metadata, exception) -> {
                if(exception != null) {
                    System.out.printf("Producing records encountered error %s %n", exception);
                } else {
                    System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
                }

            };

            var rawRecords = List.of("orderNumber-9400","orderNumber-9500");
            var producerRecords = rawRecords.stream().map(r -> new ProducerRecord<String, String>(inputTopic,"order-key", r)).collect(Collectors.toList());
            producerRecords.forEach((pr -> producer.send(pr, callback)));


        }
    }
}
