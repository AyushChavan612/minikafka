package com.minikafka.common.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPartitioner {
    
    // Used for Thread-Safe Round Robin
    private static final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Determines which partition a message should go to.
     * @param key The message key (can be null)
     * @param numPartitions The total number of partitions for the topic
     * @return The partition ID
     */
    static public int partition(String key, int numPartitions) {
        if (key == null || key.isEmpty()) {
            // No Key: Use Round-Robin
            // Math.abs handles integer overflow if the counter goes negative
            return Math.abs(counter.getAndIncrement()) % numPartitions;
        } else {
            // Key Present: Use xxHash32 for deterministic routing
            byte[] keyBytes = key.getBytes();
            int hash = XXHash32.hash(keyBytes, 0); // 0 is our constant seed
            
            // Bitwise AND with 0x7FFFFFFF forces the hash to be positive 
            return (hash & 0x7FFFFFFF) % numPartitions;
        }
    }
}