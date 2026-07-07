package com.minikafka.client;

import java.io.IOException;
import java.util.UUID; // Added for unique consumer IDs

public class HighLevelConsumer {
    private final String groupId;
    private final String topic;
    private final String consumerId; // Added to track this specific instance
    private final int partitionId; 
    private final HighLevelNetworkClient networkClient;
    private long currentOffset = -1;

    public HighLevelConsumer(String host, int port, String groupId, String topic) throws IOException {
        this.groupId = groupId;
        this.topic = topic;

        // 1. Generate a unique ID so the Broker knows exactly who this is
        this.consumerId = UUID.randomUUID().toString();
        
        this.networkClient = new HighLevelNetworkClient(host, port);
        
        // 2. REAL KAFKA REBALANCE: Ask the broker for a partition instead of guessing!
        this.partitionId = networkClient.joinGroup(topic, groupId, consumerId);
        
        System.out.println("[CLIENT] Successfully joined group. Broker assigned Partition: " + partitionId);
    }

    /**
     * Standard poll method: Fetches one record and increments offset if successful.
     */
    public String poll() throws IOException {
        if (currentOffset == -1) {
            currentOffset = networkClient.fetchOffset(groupId, topic, partitionId);
            System.out.println("[CLIENT] Synced with Server. Group '" + groupId + "' is starting at offset: " + currentOffset);
        }
        
        String payload = networkClient.sendFetchRequest(topic, partitionId, currentOffset);
        
        if (payload != null) {
            long nextOffset = currentOffset + 1;
            
            networkClient.commitOffset(groupId, topic, partitionId, nextOffset);
            
            currentOffset = nextOffset;
            return payload;
        }
        
        return null;
    }

    /**
     * REAL KAFKA BEHAVIOR: 
     * Infinitely loops to listen for new events. 
     * Sleeps for 100ms if no data is found to prevent spamming the CPU/Network.
     */
    public void startContinuousPolling() throws IOException {
        System.out.println("Starting continuous polling for topic: " + topic + " on partition " + partitionId + "...");
        
        try {
            while (true) {
                String payload = poll();
                
                if (payload != null) {
                    System.out.println("NEW EVENT PROCESSED: " + payload);
                } else {
                    // No data found. Sleep briefly, then ask again.
                    Thread.sleep(100); 
                }
            }
        } catch (InterruptedException e) {
            System.err.println("Polling thread was interrupted.");
            Thread.currentThread().interrupt();
        }
    }

    public void close() throws IOException {
        networkClient.close();
    }
}