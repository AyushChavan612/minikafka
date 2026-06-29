package com.minikafka.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class MiniKafkaConsumerTest {

    @Test
    void testProduceAndFetchMiddleRecord() {
        assertDoesNotThrow(() -> {
            String topic = "system-events";
            String routingKey = "user-123"; 
            
            // Assuming 3 partitions, find out where "user-123" will be routed
            int expectedPartition = (routingKey.hashCode() & 0x7fffffff) % 3;
            System.out.println("TEST: Routing key '" + routingKey + "' hashes to Partition " + expectedPartition);

            // 1. Produce 3 sequential records (Offsets 0, 1, 2)
            MiniKafkaProducer producer = new MiniKafkaProducer("localhost", 9092);
            producer.send(topic, routingKey, "Payload 0 (First)".getBytes());
            producer.send(topic, routingKey, "Payload 1 (Middle)".getBytes());
            producer.send(topic, routingKey, "Payload 2 (Last)".getBytes());
            producer.close();

            // Give the Broker a fraction of a second to flush to disk
            Thread.sleep(200);

            // 2. Fetch the middle record (Offset 1)
            MiniKafkaConsumer consumer = new MiniKafkaConsumer("localhost", 9092);
            System.out.println("TEST: Requesting Offset 1 from Partition " + expectedPartition + "...");
            
            consumer.fetch(topic, expectedPartition, 1); 
            consumer.close();
        });
    }
}