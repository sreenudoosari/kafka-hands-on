package com.sd.kafka.streams.split;

import com.sd.kafka.avro.ActingEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class TopicLoader {

    public static void main(String[] args) {
        runProducer();
    }

    public static void runProducer() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "acting-topic-loader");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8085");


        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                System.out.printf("Error producing record %s%n", exception);
            } else {
                System.out.printf("Record produced -> offset=%d timestamp=%d%n", metadata.offset(), metadata.timestamp());
            }
        };

        try (Admin adminClient = Admin.create(properties);
             Producer<String, SpecificRecord> producer = new KafkaProducer<>(properties)) {

            String INPUT_TOPIC = "acting.input.topic";
            String OUTPUT_TOPIC_DRAMA = "acting.output.drama";
            String OUTPUT_TOPIC_FANTASY = "acting.output.fantasy";
            String OUTPUT_TOPIC_OTHER = "acting.output.other";

            var topics = List.of(
                    new NewTopic(INPUT_TOPIC, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_DRAMA, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_FANTASY, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_OTHER, 1, (short) 1)
            );
            adminClient.createTopics(topics);

            ActingEvent dramaEvent = ActingEvent.newBuilder()
                    .setActor("Actor A")
                    .setGenre("drama")
                    .setTitle("A Streetcar Named Desire")
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build();

            ActingEvent fantasyEvent = ActingEvent.newBuilder()
                    .setActor("Actor B")
                    .setGenre("fantasy")
                    .setTitle("The Lord of the Rings")
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build();

            ActingEvent otherEvent = ActingEvent.newBuilder()
                    .setActor("Actor C")
                    .setGenre("comedy")
                    .setTitle("Funny Business")
                    .setTimestamp(Instant.now().toEpochMilli())
                    .build();

            var events = List.of(dramaEvent, fantasyEvent, otherEvent);

            events.forEach(event -> {
                ProducerRecord<String, SpecificRecord> record =
                        new ProducerRecord<>(INPUT_TOPIC, event.getActor().toString(), event);
                producer.send(record, callback);
            });
        }
    }
}
