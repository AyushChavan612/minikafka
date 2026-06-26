package com.minikafka.broker.storage;

import com.minikafka.common.model.LogRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class StorageIntegrationTest {

    @Test
    void writerAndReaderMustSerializeAndDeserializeCorrectly() throws IOException {
        String userHome = System.getProperty("user.home");
        Path tempLogFile = java.nio.file.Paths.get(userHome, "minikafka-test-partition.log");

        String topic = "system-events";
        String key = "event-001";
        byte[] payload = "critical-system-payload".getBytes();
        long offset = 42L;
        long timestamp = System.currentTimeMillis();
        int partition = 1;

        LogRecord originalRecord = new LogRecord(offset, timestamp, topic, partition, key, payload);

        PartitionWriter writer = new PartitionWriter(tempLogFile);
        writer.append(originalRecord);
        writer.close();

        PartitionReader reader = new PartitionReader(tempLogFile);
        LogRecord retrievedRecord = reader.readRecordAt(0L);
        reader.close();

        assertNotNull(retrievedRecord, "Retrieved record should not be null");
        assertEquals(originalRecord.getOffset(), retrievedRecord.getOffset());
        assertEquals(originalRecord.getTimestamp(), retrievedRecord.getTimestamp());
        assertEquals(originalRecord.getTopic(), retrievedRecord.getTopic());
        assertEquals(originalRecord.getPartition(), retrievedRecord.getPartition());
        assertEquals(originalRecord.getKey(), retrievedRecord.getKey());
        assertArrayEquals(originalRecord.getPayload(), retrievedRecord.getPayload());

        Files.deleteIfExists(tempLogFile);
    }
}