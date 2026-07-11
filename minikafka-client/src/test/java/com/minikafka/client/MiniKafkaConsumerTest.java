package com.minikafka.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class MiniKafkaConsumerTest {

    @Test
    void testHighLevelConsumerPolling() {
        assertDoesNotThrow(() -> {
            String topic = "system-events";
            String groupId = "test-group-1";
            
            System.out.println("--- STARTING NEW ARCHITECTURE TEST ---");

            // 1. Start the Consumer in a Background Thread
            // We do this because startPolling() has an infinite while(true) loop!
            Thread consumerThread = new Thread(() -> {
                try {
                    MiniKafkaConsumer consumer = new MiniKafkaConsumer("localhost", 9092);
                    
                    // This calls HighLevelConsumer -> HighLevelNetworkClient -> Broker (JOIN_GROUP)
                    consumer.subscribe(groupId, topic);
                    
                    // Starts the continuous polling loop
                    consumer.startPolling(); 
                } catch (Exception e) {
                    System.err.println("Consumer thread error: " + e.getMessage());
                }
            });
            consumerThread.start();

            // Give the consumer 500ms to connect to the broker and join the group
            Thread.sleep(500);

            // 2. Produce records to trigger the consumer's polling loop
            System.out.println("\n[TEST] Producer sending messages...");
            MiniKafkaProducer producer = new MiniKafkaProducer("localhost", 9092);
            producer.send(topic, "user-123", "Payload 0 (First)".getBytes());
            producer.send(topic, "user-123", "Payload 1 (Middle)".getBytes());
            producer.send(topic, "user-123", "Payload 2 (Last)".getBytes());
            // 3. Wait a moment to watch the Consumer automatically detect and print the messages
            Thread.sleep(10000);
            producer.close();
            
            // 4. Clean up: Interrupt the infinite loop to end the JUnit test cleanly
            System.out.println("\n[TEST] Shutting down consumer thread...");
            consumerThread.interrupt();
        });
    }
}