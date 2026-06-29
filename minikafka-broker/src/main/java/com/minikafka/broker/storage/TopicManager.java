package com.minikafka.broker.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.minikafka.common.model.LogRecord;

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
        Partition partition = topics.get(topic).get(partitionId);
        if (partition != null) {
            return partition.fetchRecord(offset);
        }
        return null;
    }
}