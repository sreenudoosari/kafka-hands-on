package com.sd.kafka.streams.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class WidgetJsonDeserializer implements Deserializer<Widget> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Widget deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Widget.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Widget", e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }
}
