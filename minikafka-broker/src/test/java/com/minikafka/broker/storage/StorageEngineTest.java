package com.minikafka.broker.storage;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineTest {

    @Test
    void testDynamicBrokerStorageAllocation() {
        int dummyBrokerId = 99; 
        String topic = "test-storage-topic";
        int partitionId = 0;

        // 1. Initialize a partition with our dummy Broker ID
        Partition partition = new Partition(topic, partitionId, dummyBrokerId);
        
        // 2. Define exactly where we expect the files to be generated
        Path expectedDir = Paths.get("/tmp/minikafka/broker-99/logs");
        Path expectedLog = expectedDir.resolve(topic + "-" + partitionId + ".log");
        Path expectedIndex = expectedDir.resolve(topic + "-" + partitionId + ".index");

        // 3. Assert that the directories and files were dynamically created in the correct isolated path!
        assertTrue(Files.exists(expectedDir), "Broker-specific log directory should exist at /tmp/minikafka/broker-99/logs");
        assertTrue(Files.exists(expectedLog), "Partition log file should exist");
        assertTrue(Files.exists(expectedIndex), "Partition index file should exist");
    }
}