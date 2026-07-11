package com.minikafka.broker.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Deque; // CHANGED to Deque
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque; // CHANGED to ConcurrentLinkedDeque

public class GroupCoordinator {

    // Structure: GroupID -> (Topic -> (PartitionID -> Offset))
    private final Map<String, Map<String, Map<Integer, Long>>> groupOffsets;
    
    // Topic -> (GroupID -> (ConsumerID -> ConsumerState))
    private final Map<String, Map<String, Map<String, ConsumerState>>> activeConsumers;
    
    // CHANGED: Topic -> (GroupID -> Deque of Available Partitions)
    private final Map<String, Map<String, Deque<Integer>>> availablePartitions;
    
    private final Path offsetLogPath;
    private FileChannel appendChannel;
    private boolean isRunning = true;

    // Helper class to store both the heartbeat time and the partition ID in RAM
    private static class ConsumerState {
        long lastHeartbeat;
        int assignedPartition;

        ConsumerState(long lastHeartbeat, int assignedPartition) {
            this.lastHeartbeat = lastHeartbeat;
            this.assignedPartition = assignedPartition;
        }
    }

    public GroupCoordinator() {
        this.groupOffsets = new ConcurrentHashMap<>();
        this.activeConsumers = new ConcurrentHashMap<>();
        this.availablePartitions = new ConcurrentHashMap<>();
        
        this.offsetLogPath = Paths.get("/home/pacforever/Documents/minikafka-logs/__consumer_offsets.txt");

        try {
            if (!Files.exists(offsetLogPath)) {
                Files.createDirectories(offsetLogPath.getParent());
                Files.createFile(offsetLogPath);
            }

            loadOffsetsFromDisk();

            this.appendChannel = FileChannel.open(offsetLogPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);

        } catch (IOException e) {
            System.err.println("CRITICAL: Failed to initialize offset storage: " + e.getMessage());
        }
        startDeadConsumerReaper();
    }

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

    public void commitOffset(String groupId, String topic, int partitionId, long offset) {
        groupOffsets
                .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .put(partitionId, offset);

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

    public synchronized int registerConsumer(String groupId, String topic, String consumerId, int numPartitions) {
        // 1. Initialize the DEQUE for this specific Topic + Group
        Deque<Integer> partitionsQueue = availablePartitions
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(groupId, k -> {
                    Deque<Integer> q = new ConcurrentLinkedDeque<>();
                    for (int i = 0; i < numPartitions; i++) {
                        q.addLast(i); // Initial setup adds to the back: 0, 1, 2
                    }
                    return q;
                });

        Map<String, ConsumerState> members = activeConsumers
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>());

        // 2. If consumer is already registered, just update heartbeat and return their existing partition
        if (members.containsKey(consumerId)) {
            ConsumerState state = members.get(consumerId);
            state.lastHeartbeat = System.currentTimeMillis();
            return state.assignedPartition;
        }

        // 3. New consumer: Take a partition from the FRONT of the deque
        Integer assignedPartition = partitionsQueue.pollFirst();
        
        if (assignedPartition == null) {
            System.err.printf("[COORDINATOR] Warning: No available partitions for Group '%s' on Topic '%s'%n", groupId, topic);
            return -1;
        }

        // 4. Save both the heartbeat time and the newly assigned partition
        members.put(consumerId, new ConsumerState(System.currentTimeMillis(), assignedPartition));

        System.out.printf("[COORDINATOR] Consumer '%s' assigned Partition: %d (Active in Group: %d)%n",
                consumerId, assignedPartition, members.size());

        return assignedPartition;
    }

    public void recordHeartbeat(String groupId, String topic, String consumerId) {
        Map<String, Map<String, ConsumerState>> topicGroups = activeConsumers.get(topic);
        if (topicGroups != null) {
            Map<String, ConsumerState> members = topicGroups.get(groupId);
            if (members != null && members.containsKey(consumerId)) {
                members.get(consumerId).lastHeartbeat = System.currentTimeMillis();
            }
        }
    }

    private void startDeadConsumerReaper() {
        Thread reaperThread = new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(5000); 
                    long now = System.currentTimeMillis();

                    for (String topic : activeConsumers.keySet()) {
                        for (String groupId : activeConsumers.get(topic).keySet()) {
                            Map<String, ConsumerState> members = activeConsumers.get(topic).get(groupId);
                            Deque<Integer> partitionsQueue = availablePartitions.get(topic).get(groupId);

                            Iterator<Map.Entry<String, ConsumerState>> iterator = members.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, ConsumerState> entry = iterator.next();
                                ConsumerState state = entry.getValue();

                                if ((now - state.lastHeartbeat) > 10000) {
                                    System.out.printf("[COORDINATOR] Evicted DEAD consumer '%s' from group '%s'%n", entry.getKey(), groupId);
                                    
                                    // THE FIX: Push the dead consumer's partition back to the FRONT of the queue!
                                    if (partitionsQueue != null) {
                                        partitionsQueue.addFirst(state.assignedPartition);
                                        System.out.printf("[COORDINATOR] Partition %d returned to FRONT of available queue.%n", state.assignedPartition);
                                    }
                                    
                                    iterator.remove();
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        reaperThread.setDaemon(true); 
        reaperThread.start();
    }

    public void close() {
        isRunning = false;
        try {
            if (appendChannel != null && appendChannel.isOpen()) {
                appendChannel.close();
            }
        } catch (IOException e) {
            System.err.println("Failed to close offset channel: " + e.getMessage());
        }
    }
}
