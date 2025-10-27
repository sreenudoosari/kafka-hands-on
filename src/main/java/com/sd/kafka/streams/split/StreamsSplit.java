package com.sd.kafka.streams.split;

import com.sd.kafka.avro.ActingEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsSplit {

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static void main(String[] args) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "splitting-streams");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085");
        streamsProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        StreamsBuilder builder = new StreamsBuilder();

        String INPUT_TOPIC = "acting.input.topic";
        String OUTPUT_TOPIC_DRAMA = "acting.output.drama";
        String OUTPUT_TOPIC_FANTASY = "acting.output.fantasy";
        String OUTPUT_TOPIC_OTHER = "acting.output.other";

        Map<String, Object> configMap = new HashMap<>();
        streamsProps.forEach((k, v) -> configMap.put(k.toString(), v));

        SpecificAvroSerde<ActingEvent> actingEventSerde = getSpecificAvroSerde(configMap);

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), actingEventSerde))
                .split()
                .branch((key, appearance) -> "drama".equalsIgnoreCase(appearance.getGenre().toString()),
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_DRAMA)))
                .branch((key, appearance) -> "fantasy".equalsIgnoreCase(appearance.getGenre().toString()),
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_FANTASY)))
                .branch((key, appearance) -> true,
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_OTHER)));

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                latch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
