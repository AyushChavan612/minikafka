package com.minikafka.client;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerRebalanceTest {

    @Test
    void testDynamicPartitionAssignment() throws Exception {
        System.out.println("--- TESTING CONSUMER REBALANCE PROTOCOL ---");
        
        // NOTE: This test requires your MiniKafkaServer to be RUNNING on port 9092!
        String host = "localhost";
        int port = 9092;
        String testTopic = "system-events";
        String testGroupId = "analytics-team";

        // 1. Create 3 separate network clients (Simulating 3 different consumers)
        HighLevelNetworkClient client1 = new HighLevelNetworkClient(host, port);
        HighLevelNetworkClient client2 = new HighLevelNetworkClient(host, port);
        HighLevelNetworkClient client3 = new HighLevelNetworkClient(host, port);
        HighLevelNetworkClient client4 = new HighLevelNetworkClient(host, port);

        try {
            // 2. Consumer 1 Joins
            String consumerId1 = UUID.randomUUID().toString();
            int partition1 = client1.joinGroup(testTopic, testGroupId, consumerId1);
            System.out.println("Consumer 1 joined. Assigned Partition: " + partition1);

            // 3. Consumer 2 Joins
            String consumerId2 = UUID.randomUUID().toString();
            int partition2 = client2.joinGroup(testTopic, testGroupId, consumerId2);
            System.out.println("Consumer 2 joined. Assigned Partition: " + partition2);

            // 4. Consumer 3 Joins
            String consumerId3 = UUID.randomUUID().toString();
            int partition3 = client3.joinGroup(testTopic, testGroupId, consumerId3);
            System.out.println("Consumer 3 joined. Assigned Partition: " + partition3);

            // 5. Consumer 4 Joins (Should wrap around back to 0!)
            String consumerId4 = UUID.randomUUID().toString();
            int partition4 = client4.joinGroup(testTopic, testGroupId, consumerId4);
            System.out.println("Consumer 4 joined. Assigned Partition: " + partition4);

            // ASSERTS: Verify the Broker is assigning sequentially (0, 1, 2, 0...)
            // Since this is the first time the group connects, it should start at 0.
            assertEquals(0, partition1, "First consumer should get Partition 0");
            assertEquals(1, partition2, "Second consumer should get Partition 1");
            assertEquals(2, partition3, "Third consumer should get Partition 2");
            assertEquals(0, partition4, "Fourth consumer should wrap around to Partition 0");

        } finally {
            client1.close();
            client2.close();
            client3.close();
            client4.close();
        }
        
        System.out.println("--- REBALANCE TEST PASSED ---");
    }
}

