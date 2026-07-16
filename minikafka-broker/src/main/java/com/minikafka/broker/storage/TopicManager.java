package com.minikafka.broker.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.minikafka.common.model.LogRecord;
import com.minikafka.common.utils.DefaultPartitioner;

public class TopicManager {

    private final ConcurrentHashMap<String, List<Partition>> topics = new ConcurrentHashMap<>();
    private final int defaultPartitions;

    public TopicManager(int defaultPartitions) {
        this.defaultPartitions = defaultPartitions;
    }

    private void getOrCreateTopic(String topicName) {
        topics.computeIfAbsent(topicName, t -> {
            List<Partition> partitions = new ArrayList<>();
            for (int i = 0; i < defaultPartitions; i++) {
                partitions.add(new Partition(topicName, i));
            }
            System.out.println(
                    "--> Created new topic system: [" + topicName + "] with " + defaultPartitions + " partitions.");
            return partitions;
        });
    }

    public void routeRecord(String topic, String key, String payload) {
        // 1. Ensure the topic exists in memory
        getOrCreateTopic(topic);

        // 2. Fetch the partitions for this topic
        List<Partition> partitions = topics.get(topic);

        // 3. Math routing: Find the exact partition index for this key
        int partitionIndex = DefaultPartitioner.partition(key, partitions.size());

        // 4. Hand the data off to that specific partition object
        Partition targetPartition = partitions.get(partitionIndex);
        targetPartition.append(key, payload);
    }

    public LogRecord fetchRecord(String topic, int partitionId, long offset) {
        // 1. Fetch the list safely without chaining .get()
        List<Partition> topicPartitions = topics.get(topic);
        
        // 2. If topic doesn't exist yet, gracefully return null (Consumer will just sleep and retry)
        if (topicPartitions == null) {
            return null;
        }
        
        // 3. If partitionId is out of bounds, gracefully return null
        if (partitionId < 0 || partitionId >= topicPartitions.size()) {
            return null;
        }
        
        // 4. Safely get the specific partition and fetch the data
        Partition partition = topicPartitions.get(partitionId);
        return partition.fetchRecord(offset);
    }

    public void appendToPartition(String topic, int partitionId, String key, String payload) {
        // 1. Ensure the topic exists in memory
        getOrCreateTopic(topic);

        // 2. Fetch the partitions for this topic
        java.util.List<Partition> partitions = topics.get(topic);

        // 3. Safety check, then hand the data off to that specific partition object
        if (partitionId >= 0 && partitionId < partitions.size()) {
            Partition targetPartition = partitions.get(partitionId);
            targetPartition.append(key, payload);
        } else {
            System.err.println("Broker Error: Producer requested invalid partition ID: " + partitionId);
        }
    }
}