package com.sd.kafka.streams.basic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaWithoutStreams {

    public static void main(String[] args) {
        try (Consumer<String, Widget> consumer = new KafkaConsumer<>(consumerProperties());
             Producer<String, Widget> producer = new KafkaProducer<>(producerProperties())) {
            consumer.subscribe(Collections.singletonList("widgets"));
            System.out.println("Listening for widgets...");
            while (true) {
                ConsumerRecords<String, Widget> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, Widget> record : records) {
                    Widget widget = record.value();
                    System.out.println("Consumed widget: " + widget);
                    if (widget.colour().equalsIgnoreCase("red")) {
                        ProducerRecord<String, Widget> producerRecord =
                                new ProducerRecord<>("widgets-red-simple", record.key(), widget);
                        producer.send(producerRecord, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.printf("Forwarded to widgets-red topic (partition=%d, offset=%d)%n",
                                        metadata.partition(), metadata.offset());
                            } else {
                                exception.printStackTrace();
                            }
                        });
                    }
                }
            }
        }
    }

    private static Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "widget-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.sd.kafka.streams.basic.WidgetJsonDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.sd.kafka.streams.basic.WidgetJsonSerializer");
        return props;
    }
}
