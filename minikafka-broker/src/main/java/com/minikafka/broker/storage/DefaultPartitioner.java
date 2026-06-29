package com.minikafka.broker.storage;

public class DefaultPartitioner {
    
    public static int partition(String key, int numPartitions) {
        if (key == null) {
            return 0;
        }
        
        // Bitwise trick to guarantee a positive integer, then modulo by partitions
        return (key.hashCode() & 0x7fffffff) % numPartitions;
    }
}