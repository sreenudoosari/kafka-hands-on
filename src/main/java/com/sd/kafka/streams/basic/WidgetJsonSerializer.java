package com.sd.kafka.streams.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class WidgetJsonSerializer implements Serializer<Widget> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Widget data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Widget", e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }
}
