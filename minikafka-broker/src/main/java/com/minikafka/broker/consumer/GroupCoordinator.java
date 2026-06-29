package com.minikafka.broker.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GroupCoordinator {
    
    // Structure: GroupID -> (Topic -> (PartitionID -> Offset))
    private final Map<String, Map<String, Map<Integer, Long>>> groupOffsets;

    public GroupCoordinator() {
        this.groupOffsets = new ConcurrentHashMap<>();
    }

    /**
     * Called when a consumer wants to save its progress.
     */
    public void commitOffset(String groupId, String topic, int partitionId, long offset) {
        groupOffsets
            .computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
            .put(partitionId, offset);
            
        System.out.printf("[COORDINATOR] Group '%s' committed Offset %d for Topic '%s' Partition %d%n", 
                groupId, offset, topic, partitionId);
                
        // In Phase 3, we will append this commit to the __consumer_offsets disk log here!
    }

    /**
     * Called when a consumer connects and asks: "Where did I leave off?"
     * Returns 0 if the group has never consumed this partition before.
     */
    public long fetchOffset(String groupId, String topic, int partitionId) {
        Map<String, Map<Integer, Long>> topicsForGroup = groupOffsets.get(groupId);
        if (topicsForGroup != null) {
            Map<Integer, Long> partitionsForTopic = topicsForGroup.get(topic);
            if (partitionsForTopic != null) {
                return partitionsForTopic.getOrDefault(partitionId, 0L);
            }
        }
        return 0L; // Default to starting at the beginning
    }
}