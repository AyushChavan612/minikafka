package com.minikafka.broker.storage;

import com.minikafka.common.model.LogRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {
    private final String topic;
    private final int partitionId;
    private final AtomicLong currentOffset;
    private final PartitionWriter writer;

    public Partition(String topic, int partitionId) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.currentOffset = new AtomicLong(0);

        try {
            Path logDirectory = Paths.get("/home/pacforever/Documents/minikafka-logs");
            if (!Files.exists(logDirectory)) {
                Files.createDirectories(logDirectory);
            }

            Path partitionPath = logDirectory.resolve(topic + "-" + partitionId + ".log");
            Path indexPath = logDirectory.resolve(topic + "-" + partitionId + ".index"); 

            this.writer = new PartitionWriter(partitionPath, indexPath);
            System.out.println("--> Initialized physical storage at: " + partitionPath);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize PartitionWriter for topic: " + topic, e);
        }
    }

    public void append(String key, String payload) {
        long offset = currentOffset.getAndIncrement();
        long timestamp = System.currentTimeMillis();

        // Convert the String payload to byte[] as expected by your LogRecord
        byte[] payloadBytes = payload != null ? payload.getBytes() : new byte[0];

        LogRecord record = new LogRecord(
                offset,
                timestamp,
                topic,
                partitionId,
                key,
                payloadBytes);

        try {
            // 5. Write to the physical disk!
            writer.append(record);
            System.out.printf("[SAVED TO DISK] Topic: %s | Partition: %d | Offset: %d%n",
                    topic, partitionId, offset);
        } catch (IOException e) {
            System.err.println("CRITICAL: Failed to write record to disk - " + e.getMessage());
        }
    }
}