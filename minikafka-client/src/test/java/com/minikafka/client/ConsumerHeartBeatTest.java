package com.minikafka.client;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerHeartBeatTest {

    @Test
    void testQueueBasedEviction() throws Exception {
        System.out.println("--- TESTING QUEUE-BASED CONSUMER EVICTION ---");
        
        String host = "localhost";
        int port = 9092;
        String topic = "system-events";
        String groupId = "queue-test-group-" + UUID.randomUUID().toString().substring(0, 5); 

        // 1. Connect Consumer 1
        HighLevelNetworkClient client1 = new HighLevelNetworkClient(host, port);
        String consumerId1 = UUID.randomUUID().toString();
        int partition1 = client1.joinGroup(topic, groupId, consumerId1);
        
        System.out.println("Consumer 1 joined. Assigned Partition: " + partition1);
        assertEquals(0, partition1, "First consumer must get Partition 0 from the queue");

        // 2. Simulate a crash / network drop
        System.out.println("Simulating Consumer 1 crash... Waiting 18 seconds for Broker to reap it and return partition to queue...");
        for (int i = 18; i > 0; i--) {
            System.out.print(i + "... ");
            Thread.sleep(1000);
        }
        System.out.println("\nTime's up! Connecting Consumer 2...");

        // 3. Connect Consumer 2
        HighLevelNetworkClient client2 = new HighLevelNetworkClient(host, port);
        String consumerId2 = UUID.randomUUID().toString();
        
        // This will trigger the pre-emptive cleanup in the Broker before assigning!
        int partition2 = client2.joinGroup(topic, groupId, consumerId2);
        
        System.out.println("Consumer 2 joined. Assigned Partition: " + partition2);

        // 4. The Ultimate Assertion
        assertEquals(0, partition2, "Consumer 2 should get Partition 0 because it was returned to the queue!");

        // Cleanup
        client1.close();
        client2.close();
        
        System.out.println("--- QUEUE EVICTION TEST PASSED ---");
    }
}
