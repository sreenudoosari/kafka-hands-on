package com.sd.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DemoProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.println("Message sent asynchronously:");
            System.out.println("  Topic: " + metadata.topic());
            System.out.println("  Partition: " + metadata.partition());
            System.out.println("  Offset: " + metadata.offset());
        } else {
            System.err.println("Error while sending message:");
            exception.printStackTrace();
        }
    }
}
