package com.sd.kafka.streams.kafkaconnect;

public record PGUser(
    Integer id,
    String name,
    String email
) {}