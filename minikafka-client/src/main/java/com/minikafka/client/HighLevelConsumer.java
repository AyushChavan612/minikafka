package com.minikafka.client;

import java.io.IOException;
import java.util.UUID;

public class HighLevelConsumer {
    private final String groupId;
    private final String topic;
    private final String consumerId;
    private final int partitionId;
    private long lastHeartbeatTime = 0;
    private final HighLevelNetworkClient networkClient;
    private long currentOffset = -1;

    // Notice we pass the networkClient in the constructor to connect them
    public HighLevelConsumer(HighLevelNetworkClient networkClient, String groupId, String topic) throws IOException {
        this.networkClient = networkClient;
        this.groupId = groupId;
        this.topic = topic;
        this.consumerId = UUID.randomUUID().toString();

        // NO MANUAL ASSIGNMENT: We strictly ask the Broker, utilizing your xxHash32
        // logic!
        this.partitionId = networkClient.joinGroup(topic, groupId, consumerId);

        System.out.println("[HIGH-LEVEL] Successfully joined group. Broker assigned Partition: " + partitionId);
    }

    public String poll() throws IOException {
        if (currentOffset == -1) {
            currentOffset = networkClient.fetchOffset(groupId, topic, partitionId);
            System.out.println(
                    "[HIGH-LEVEL] Synced with Server. Group '" + groupId + "' starting at offset: " + currentOffset);
        }

        String payload = networkClient.sendFetchRequest(topic, partitionId, currentOffset);

        System.out.println(payload);
        System.out.println("currentOffset: " + currentOffset);
        if (payload != null) {
            long nextOffset = currentOffset + 1;
            networkClient.commitOffset(groupId, topic, partitionId, nextOffset);
            currentOffset = nextOffset;
            return payload;
        }

        return null;
    }

    public void startContinuousPolling() throws IOException {
        System.out.println("Starting continuous polling for topic: " + topic + " on partition " + partitionId + "...");
        try {
            while (true) {
                // --- HEARTBEAT LOGIC ---
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatTime > 3000) { // Send heartbeat every 3 seconds
                    networkClient.sendHeartbeat(topic, groupId, consumerId);
                    lastHeartbeatTime = now;
                }

                String payload = poll();
                if (payload != null) {
                    System.out.println("NEW EVENT PROCESSED: " + payload);
                } else {
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            System.err.println("Polling thread was interrupted.");
            Thread.currentThread().interrupt();
        }
    }
}
