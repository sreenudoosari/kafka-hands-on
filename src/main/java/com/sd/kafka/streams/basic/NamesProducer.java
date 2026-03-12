package com.sd.kafka.streams.basic;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.List;

public class NamesProducer {

    public static void main(String[] args) {
        String topic = "names";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        List<String> names = List.of(
                " vlad put" + RandomStringUtils.randomAlphabetic(4),
                "bob"+ RandomStringUtils.randomAlphabetic(4),
                "vlad putin charlie"+RandomStringUtils.randomAlphabetic(4)
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int key = 1;
            for (String name : names) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, String.valueOf(key), name);
                producer.send(record);
                key++;
            }

            producer.flush();
        }
    }
}