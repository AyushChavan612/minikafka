package com.minikafka.broker.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.minikafka.common.model.LogRecord;
import com.minikafka.common.utils.DefaultPartitioner;

public class TopicManager {

    private final ConcurrentHashMap<String, List<Partition>> topics = new ConcurrentHashMap<>();
    private final int defaultPartitions;
    private final int brokerId; // <-- ADDED

    // <-- UPDATED: Now requires brokerId
    public TopicManager(int defaultPartitions, int brokerId) { 
        this.defaultPartitions = defaultPartitions;
        this.brokerId = brokerId;
    }

    private void getOrCreateTopic(String topicName) {
        topics.computeIfAbsent(topicName, t -> {
            List<Partition> partitions = new ArrayList<>();
            for (int i = 0; i < defaultPartitions; i++) {
                // <-- UPDATED: Passing the brokerId down to the Partition
                partitions.add(new Partition(topicName, i, this.brokerId)); 
            }
            System.out.println(
                    "--> Created new topic system: [" + topicName + "] with " + defaultPartitions + " partitions.");
            return partitions;
        });
    }

    public void routeRecord(String topic, String key, String payload) {
        getOrCreateTopic(topic);
        List<Partition> partitions = topics.get(topic);
        int partitionIndex = DefaultPartitioner.partition(key, partitions.size());
        Partition targetPartition = partitions.get(partitionIndex);
        targetPartition.append(key, payload);
    }

    public LogRecord fetchRecord(String topic, int partitionId, long offset) {
        List<Partition> topicPartitions = topics.get(topic);
        if (topicPartitions == null) {
            return null;
        }
        if (partitionId < 0 || partitionId >= topicPartitions.size()) {
            return null;
        }
        Partition partition = topicPartitions.get(partitionId);
        return partition.fetchRecord(offset);
    }
}