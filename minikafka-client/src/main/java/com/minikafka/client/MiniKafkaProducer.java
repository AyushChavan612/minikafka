package com.minikafka.client;

import com.minikafka.common.utils.DefaultPartitioner;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MiniKafkaProducer {
    private final HighLevelNetworkClient controllerClient;
    private final Map<Integer, Integer> clusterMetadata; // Partition ID -> Leader Port
    private final int numPartitions;
    
    // Connection Pool: We keep open sockets to any Worker we talk to
    private final Map<Integer, HighLevelNetworkClient> brokerConnections = new HashMap<>();

    public MiniKafkaProducer(String controllerHost, int controllerPort) throws IOException {
        // 1. Connect to the Controller just to get the map
        this.controllerClient = new HighLevelNetworkClient(controllerHost, controllerPort);
        this.clusterMetadata = controllerClient.fetchMetadata();
        this.numPartitions = clusterMetadata.size();
        
        System.out.println("[PRODUCER] Downloaded Cluster Metadata: " + clusterMetadata);
    }

    public void send(String topic, String key, byte[] payload) throws IOException {
        int partitionId = DefaultPartitioner.partition(key, numPartitions);
        Integer leaderPort = clusterMetadata.get(partitionId);
        
        if (leaderPort == null) {
            throw new IOException("Partition " + partitionId + " has no known leader!");
        }

        HighLevelNetworkClient brokerClient = brokerConnections.computeIfAbsent(leaderPort, port -> {
            try {
                return new HighLevelNetworkClient("localhost", port);
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect to broker at port " + port, e);
            }
        });

        try {
            // Attempt to send
            brokerClient.sendProduceRequest(topic, key, payload);
            System.out.println("[PRODUCER] Successfully routed message to Partition " + partitionId);
            
        } catch (IOException e) {
            // THE REBALANCING SAFETY NET
            if ("NOT_LEADER".equals(e.getMessage())) {
                System.out.println("[PRODUCER] 🚨 Stale metadata detected (Broker rejected). Refreshing map...");
                
                // 1. Purge the old connection and map
                brokerClient.close();
                brokerConnections.remove(leaderPort);
                clusterMetadata.clear();
                
                // 2. Download the new map from the Controller
                clusterMetadata.putAll(controllerClient.fetchMetadata());
                
                // 3. Retry the exact same message recursively
                System.out.println("[PRODUCER] Map refreshed! Retrying message...");
                send(topic, key, payload);
            } else {
                throw e; // It was a real network crash, throw it up
            }
        }
    }

    public void close() throws IOException {
        controllerClient.close();
        for (HighLevelNetworkClient client : brokerConnections.values()) {
            client.close();
        }
    }
}