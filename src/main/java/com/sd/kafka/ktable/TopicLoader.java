package com.sd.kafka.ktable;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    public static void runProducer() throws IOException {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_topic_ktable");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try(Admin adminClient = Admin.create(properties);
            Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String inputTopic = "my_topic";
            String outputTopic = "my_topic_latest";
            NewTopic in = new NewTopic(inputTopic, 1, (short) 1);
            NewTopic out = new NewTopic(outputTopic, 1, (short) 1)
                    .configs(Map.of("cleanup.policy", "compact"));

           /* NewTopic out = new NewTopic(outputTopic, 1, (short) 1)
                    .configs(Map.of(
                            "cleanup.policy", "compact",
                            "segment.ms", "1000",                 // roll segments every 1s
                            "min.cleanable.dirty.ratio", "0.01",  // trigger compaction with 1% dirty
                            "delete.retention.ms", "1000"         // keep tombstones 1s
                    ));*/
            adminClient.createTopics(List.of(in,out));
            Callback callback = (metadata, exception) -> {
                if(exception != null) {
                    System.out.printf("Producing records encountered error %s %n", exception);
                } else {
                    System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
                }

            };

            var rawRecords = List.of("orderNumber-1001",
                                              "orderNumber-5000",
                                               "orderNumber-999",
                                               "orderNumber-3330",
                                               "bogus-1",
                                               "bogus-2",
                                               "orderNumber-8400");
            var producerRecords = rawRecords.stream().map(r -> new ProducerRecord<String, String>(inputTopic,"order-key", r)).collect(Collectors.toList());
            producerRecords.forEach((pr -> producer.send(pr, callback)));


        }
    }
}