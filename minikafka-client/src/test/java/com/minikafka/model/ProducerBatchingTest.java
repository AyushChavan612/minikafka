package com.minikafka.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import com.minikafka.client.MiniKafkaProducer;

public class ProducerBatchingTest {

    @Test
    void testProducerLingerAndTimeTriggers() {
        assertDoesNotThrow(() -> {
            System.out.println("--- TESTING PRODUCER BATCHING ALGORITHM ---");
            
            // Connect to the local Broker
            MiniKafkaProducer producer = new MiniKafkaProducer("localhost", 9092);
            
            // ==========================================
            // TEST 1: The Linger (Time) Trigger
            // ==========================================
            System.out.println("\n[TEST 1] Sending 1 small message and waiting for Linger timeout (50ms)...");
            producer.send("system-events", "key-1", "This should wait 50ms before sending".getBytes());
            
            // Sleep for 100ms. Because we aren't sending any more data, the background 
            // thread should hit the 50ms limit and flush this single message.
            Thread.sleep(100); 
            
            // ==========================================
            // TEST 2: The Size Trigger (16KB)
            // ==========================================
            System.out.println("\n[TEST 2] Spamming large messages to force a Size flush (16KB)...");
            byte[] largePayload = new byte[4000]; // 4KB per message
            
            // 5 messages * 4KB = 20KB. 
            // The background thread should see it cross 16KB on the 4th message and instantly 
            // flush without waiting for the timer! The 5th message goes into a new batch.
            for (int i = 1; i <= 5; i++) {
                producer.send("system-events", "key-2", largePayload);
                System.out.println("Queued 4KB message #" + i);
            }
            
            // Give the background thread a moment to flush the remaining 5th message
            Thread.sleep(100);
            
            producer.close();
            System.out.println("\n--- BATCHING TEST COMPLETE ---");
        });
    }
}