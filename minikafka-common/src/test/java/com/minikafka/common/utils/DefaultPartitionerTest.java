package com.minikafka.common.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultPartitionerTest {

    @Test
    void testDeterministicKeyHashing() {
        System.out.println("--- TESTING XXHASH32 DETERMINISTIC ROUTING ---");
        int numPartitions = 5;
        String key1 = "user-123";
        String key2 = "user-456";

        // Get the initial partition for key1
        int partitionForKey1 = DefaultPartitioner.partition(key1, numPartitions);
        System.out.println("Key '" + key1 + "' routed to partition: " + partitionForKey1);

        // Prove that hashing the same key 100 times ALWAYS yields the exact same partition
        for (int i = 0; i < 100; i++) {
            int p = DefaultPartitioner.partition(key1, numPartitions);
            assertEquals(partitionForKey1, p, "The same key must always hash to the same partition!");
        }

        // Test bounds: ensure a different key is routed properly and stays within 0 to (numPartitions - 1)
        int partitionForKey2 = DefaultPartitioner.partition(key2, numPartitions);
        System.out.println("Key '" + key2 + "' routed to partition: " + partitionForKey2);
        assertTrue(partitionForKey2 >= 0 && partitionForKey2 < numPartitions, "Partition ID out of bounds!");
    }

    @Test
    void testRoundRobinNoKey() {
        System.out.println("--- TESTING ROUND-ROBIN (NULL KEY) ---");
        int numPartitions = 3;

        // Fetch 5 consecutive partitions for null keys
        int p1 = DefaultPartitioner.partition(null, numPartitions);
        int p2 = DefaultPartitioner.partition(null, numPartitions);
        int p3 = DefaultPartitioner.partition(null, numPartitions);
        int p4 = DefaultPartitioner.partition(null, numPartitions);
        int p5 = DefaultPartitioner.partition(null, numPartitions);

        System.out.println("Round Robin Sequence: " + p1 + ", " + p2 + ", " + p3 + ", " + p4 + ", " + p5);

        // Because you made the counter static, the exact starting number depends on test execution order.
        // So, we test the RELATIVE sequence to prove it wraps around perfectly using modulo arithmetic.
        assertEquals((p1 + 1) % numPartitions, p2, "Round Robin failed on step 2");
        assertEquals((p2 + 1) % numPartitions, p3, "Round Robin failed on step 3");
        assertEquals((p3 + 1) % numPartitions, p4, "Round Robin failed on step 4");
        assertEquals((p4 + 1) % numPartitions, p5, "Round Robin failed on step 5");
        
        System.out.println("Round Robin sequence wraps correctly!");
    }
}