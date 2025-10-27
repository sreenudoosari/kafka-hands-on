package com.sd.kafka.producer.json;

import java.util.UUID;

public class CustomerGenerator {
    public static Customer getNext() {
        String id = UUID.randomUUID().toString();
        String name = "Customer_" + id.substring(0, 5);
        String email = name.toLowerCase() + "@example.com";
        return new Customer(id, name, email);
    }
}