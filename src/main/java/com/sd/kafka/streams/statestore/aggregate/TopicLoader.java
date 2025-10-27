package com.sd.kafka.streams.statestore.aggregate;

import com.sd.kafka.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    public static void runProducer() throws IOException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-topic-loader");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8085");

        try(Admin adminClient = Admin.create(properties);
            Producer<String, ElectronicOrder> producer = new KafkaProducer<>(properties)) {
            String INPUT_TOPIC = "aggregate.input.topic";
            String OUTPUT_TOPIC = "aggregate.output.topic";

            var topics = List.of(
                    new NewTopic(INPUT_TOPIC, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1)
            );

            adminClient.createTopics(topics);

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    System.out.printf("Error producing record %s%n", exception);
                } else {
                    System.out.printf("Record produced -> offset=%d timestamp=%d%n", metadata.offset(), metadata.timestamp());
                }
            };

            Instant instant = Instant.now();

            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("10261998")
                    .setPrice(2000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(10L);

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1033737373")
                    .setPrice(1999.23)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(10L);

            ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1026333")
                    .setPrice(4500.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(12L);

            ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(1333.98)
                    .setTime(instant.toEpochMilli()).build();


            var electronicOrders = List.of(electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour );

            electronicOrders.forEach((electronicOrder -> {

                ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord(INPUT_TOPIC,
                        0,
                        electronicOrder.getTime(),
                        electronicOrder.getElectronicId(),
                        electronicOrder);
                producer.send(producerRecord, callback);
            }));



        }
    }
}