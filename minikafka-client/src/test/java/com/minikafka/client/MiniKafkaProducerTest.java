package com.minikafka.client;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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