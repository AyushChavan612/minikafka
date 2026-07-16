package com.minikafka.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

public class MiniKafkaProducerTest {

    @Test
    void testProducerConnection() {
        assertDoesNotThrow(() -> {
            MiniKafkaProducer producer = new MiniKafkaProducer("localhost", 9092);
            
            String topic = "system-events";
            String key = "event-001";
            byte[] payload = "Hello from JUnit!".getBytes();

            producer.send(topic, key, payload);
            producer.close();
        });
    }
}