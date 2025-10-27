package com.sd.kafka.streams.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.random.RandomGenerator;

public class KafkaWidgetProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.sd.kafka.streams.basic.WidgetJsonSerializer");

        try (KafkaProducer<String, Widget> producer = new KafkaProducer<>(props)) {
            Widget widget =  randomWidget();
            ProducerRecord<String, Widget> record = new ProducerRecord<>("widgets", widget.id(), widget);
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Sent Widget to topic: " + metadata.topic() +
                    ", partition: " + metadata.partition() +
                    ", key: " + widget.id() +
                    ", value: " + widget);
        }
    }

    public static Widget randomWidget() {
        Colour colour = Colour.values()[RandomGenerator.getDefault().nextInt(2)];
        String id = colour.getId();
        int size = 999; // Random size from 0 to 99
        return new Widget(id, colour.name(), size);
    }
}


