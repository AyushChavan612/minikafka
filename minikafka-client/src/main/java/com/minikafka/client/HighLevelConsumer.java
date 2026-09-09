package com.minikafka.client;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class HighLevelConsumer {
    private final String groupId;
    private final String topic;
    private final String consumerId;
    private final int partitionId;
    private long lastHeartbeatTime = 0;
    
    // Administrative client (talks to Controller)
    private final HighLevelNetworkClient controllerClient;
    
    // Data stream client (talks directly to the Worker Leader)
    private HighLevelNetworkClient workerClient; 
    private Map<Integer, Integer> clusterMetadata;
    private long currentOffset = -1;

    public HighLevelConsumer(HighLevelNetworkClient controllerClient, String groupId, String topic) throws IOException {
        this.controllerClient = controllerClient;
        this.groupId = groupId;
        this.topic = topic;
        this.consumerId = UUID.randomUUID().toString();

        // Ask the Controller for a partition assignment
        this.partitionId = controllerClient.joinGroup(topic, groupId, consumerId);
        System.out.println("[HIGH-LEVEL] Successfully joined group. Assigned Partition: " + partitionId);
        
        refreshMetadataAndConnect();
    }

    private void refreshMetadataAndConnect() throws IOException {
        // 1. Ask Controller for the latest map
        this.clusterMetadata = controllerClient.fetchMetadata();
        
        Integer leaderPort = clusterMetadata.get(partitionId);
        if (leaderPort == null) {
            throw new IOException("Partition " + partitionId + " has no known leader!");
        }

        // 2. Drop the old worker connection if it exists
        if (this.workerClient != null) {
            this.workerClient.close();
        }
        
        // 3. Open a direct TCP pipe to the specific Worker
        this.workerClient = new HighLevelNetworkClient("localhost", leaderPort);
        System.out.println("[CONSUMER] Routed direct connection to Worker Leader on port " + leaderPort);
    }

    public String poll() throws IOException {
        if (currentOffset == -1) {
            currentOffset = controllerClient.fetchOffset(groupId, topic, partitionId);
            System.out.println("[CONSUMER] Synced with Coordinator. Starting at offset: " + currentOffset);
        }

        try {
            // Stream data DIRECTLY from the Worker
            String payload = workerClient.sendFetchRequest(topic, partitionId, currentOffset);

            if (payload != null) {
                long nextOffset = currentOffset + 1;
                // Commit offset back to the Controller
                controllerClient.commitOffset(groupId, topic, partitionId, nextOffset);
                currentOffset = nextOffset;
                return payload;
            }
        } catch (IOException e) {
            // THE REBALANCING SAFETY NET
            if ("NOT_LEADER".equals(e.getMessage())) {
                System.out.println("[CONSUMER] 🚨 Stale metadata detected (Broker rejected). Refreshing map...");
                refreshMetadataAndConnect();
                return poll(); // Retry instantly
            } else {
                throw e; 
            }
        }

        return null;
    }

    public void startContinuousPolling() throws IOException {
        System.out.println("Starting continuous polling for topic: " + topic + " on partition " + partitionId + "...");
        try {
            while (true) {
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatTime > 3000) { 
                    // Send heartbeats to the Controller to maintain group membership
                    controllerClient.sendHeartbeat(topic, groupId, consumerId);
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