package com.minikafka.broker.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GroupCoordinator {
    
    // Structure: GroupID -> (Topic -> (PartitionID -> Offset))
    private final Map<String, Map<String, Map<Integer, Long>>> groupOffsets;
    private final Path offsetLogPath;
    private FileChannel appendChannel;

    public GroupCoordinator() {
        this.groupOffsets = new ConcurrentHashMap<>();
        
        // This is saved in your HDD experimental workspace alongside your other logs
        this.offsetLogPath = Paths.get("/home/pacforever/Documents/minikafka-logs/__consumer_offsets.txt");
        
        try {
            if (!Files.exists(offsetLogPath)) {
                Files.createDirectories(offsetLogPath.getParent());
                Files.createFile(offsetLogPath);
            }
            
            // 1. On startup, read sequentially to rebuild RAM state
            loadOffsetsFromDisk();
            
            // 2. Open a fast NIO FileChannel for appending new commits
            this.appendChannel = FileChannel.open(offsetLogPath, 
                StandardOpenOption.WRITE, 
                StandardOpenOption.APPEND);
                
        } catch (IOException e) {
            System.err.println("CRITICAL: Failed to initialize offset storage: " + e.getMessage());
        }
    }

    /**
     * Reads the file on Broker startup to rebuild the in-memory map.
     * Blocking I/O (BufferedReader) is perfectly safe here because the server is booting up
     * and hasn't started accepting network connections yet.
     */
    private void loadOffsetsFromDisk() {
        try (BufferedReader reader = Files.newBufferedReader(offsetLogPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 4) {
                    String groupId = parts[0];
                    String topic = parts[1];
                    int partitionId = Integer.parseInt(parts[2]);
                    long offset = Long.parseLong(parts[3]);
                    
                    groupOffsets
                        .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                        .put(partitionId, offset);
                }
            }
            System.out.println("[COORDINATOR] Successfully loaded previous consumer offsets from disk.");
        } catch (IOException e) {
            System.err.println("Failed to read offset log: " + e.getMessage());
        }
    }

    /**
     * Saves the offset to RAM, and appends it to the disk log using non-blocking NIO.
     */
    public void commitOffset(String groupId, String topic, int partitionId, long offset) {
        // 1. Save to RAM (Instant)
        groupOffsets
            .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
            .put(partitionId, offset);
            
        // 2. Save to Disk using NIO ByteBuffer (Zero-Copy mechanics)
        try {
            String record = String.format("%s,%s,%d,%d\n", groupId, topic, partitionId, offset);
            ByteBuffer buffer = ByteBuffer.wrap(record.getBytes());
            
            while (buffer.hasRemaining()) {
                appendChannel.write(buffer);
            }
            
            System.out.printf("[COORDINATOR] Group '%s' committed Offset %d for Topic '%s' Partition %d%n", 
                groupId, offset, topic, partitionId);
        } catch (IOException e) {
            System.err.println("CRITICAL: Failed to persist offset to disk: " + e.getMessage());
        }
    }

    /**
     * Lookups are instantaneous because they hit the RAM map, never the disk.
     */
    public long fetchOffset(String groupId, String topic, int partitionId) {
        Map<String, Map<Integer, Long>> topicsForGroup = groupOffsets.get(groupId);
        if (topicsForGroup != null) {
            Map<Integer, Long> partitionsForTopic = topicsForGroup.get(topic);
            if (partitionsForTopic != null) {
                return partitionsForTopic.getOrDefault(partitionId, 0L);
            }
        }
        return 0L; 
    }
    
    public void close() {
        try {
            if (appendChannel != null && appendChannel.isOpen()) {
                appendChannel.close();
            }
        } catch (IOException e) {
            System.err.println("Failed to close offset channel: " + e.getMessage());
        }
    }
}