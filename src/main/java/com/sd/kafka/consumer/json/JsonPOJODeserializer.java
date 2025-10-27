package com.sd.kafka.consumer.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonPOJODeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetType;

    public JsonPOJODeserializer() { }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        String targetClassName = (String) props.get("json.deserializer.target.class");
        if (targetClassName != null) {
            try {
                this.targetType = (Class<T>) Class.forName(targetClassName);
            } catch (ClassNotFoundException e) {
                throw new SerializationException("Target class not found for JSON deserializer", e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;
        try {
            return objectMapper.readValue(bytes, targetType);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON message", e);
        }
    }

    @Override
    public void close() { }
}
