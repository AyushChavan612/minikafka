package com.minikafka.broker.cluster;

import com.minikafka.common.protocol.RequestCodes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class ControllerService {
    private final ClusterManager manager;

    public ControllerService(ClusterManager manager) {
        this.manager = manager;
    }

    public void generateInitialMap(int numPartitions, int numBrokers, int replicationFactor) {
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> assignedBrokers = new ArrayList<>();
            int leaderId = partitionId % numBrokers;
            assignedBrokers.add(leaderId);
            
            for (int i = 1; i < replicationFactor; i++) {
                assignedBrokers.add((leaderId + i) % numBrokers);
            }
            
            manager.clusterMap.put(partitionId, assignedBrokers);
            System.out.printf("[CONTROLLER MAP] Partition %d -> Leader: Broker %d | Followers: %s\n", 
                partitionId, leaderId, assignedBrokers.subList(1, assignedBrokers.size()));
        }
    }

    public void handleIncomingWorkerRegistration(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        int workerId = buffer.getInt();
        int workerPort = buffer.getInt();
        
        manager.activeBrokers.put(workerId, workerPort);
        manager.lastHeartbeatTimestamp.put(workerId, System.currentTimeMillis());
        manager.workerChannels.put(workerId, clientChannel);
        
        System.out.printf("[CONTROLLER] Worker Broker %d registered on port %d.\n", workerId, workerPort);
        
        boolean mapUpdated = replenishUnderReplicatedPartitions();
        
        System.out.println("[CONTROLLER] Sending cluster map back to Broker " + workerId + "...");
        
        int bufferSize = 4; 
        for (List<Integer> brokers : manager.clusterMap.values()) {
            bufferSize += 8 + (brokers.size() * 4); 
        }

        ByteBuffer responseBuffer = ByteBuffer.allocate(bufferSize);
        responseBuffer.putInt(manager.clusterMap.size()); 

        for (Map.Entry<Integer, List<Integer>> entry : manager.clusterMap.entrySet()) {
            responseBuffer.putInt(entry.getKey()); 
            List<Integer> brokers = entry.getValue();
            responseBuffer.putInt(brokers.size()); 
            for (int bId : brokers) {
                responseBuffer.putInt(bId); 
            }
        }

        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
            int bytesWritten = clientChannel.write(responseBuffer);
            if (bytesWritten == 0) {
                try { Thread.sleep(10); } catch (InterruptedException e) {}
            }
        }
        System.out.println("[CONTROLLER] Map successfully sent to Broker " + workerId);

        if (mapUpdated) {
            System.out.println("[CONTROLLER] Cluster map changed during registration. Broadcasting update...");
            broadcastClusterMap();
        }
    }

    public void handleBrokerHeartbeat(ByteBuffer buffer) {
        int workerId = buffer.getInt();
        manager.lastHeartbeatTimestamp.put(workerId, System.currentTimeMillis());
    }

    public void handleBrokerFailure(int deadBrokerId) {
        manager.activeBrokers.remove(deadBrokerId);
        manager.lastHeartbeatTimestamp.remove(deadBrokerId);
        manager.workerChannels.remove(deadBrokerId);
        
        for (Map.Entry<Integer, List<Integer>> entry : manager.clusterMap.entrySet()) {
            List<Integer> oldReplicas = entry.getValue();
            
            if (oldReplicas.contains(deadBrokerId)) {
                List<Integer> newReplicas = new ArrayList<>(oldReplicas);
                newReplicas.remove(Integer.valueOf(deadBrokerId)); 
                
                manager.clusterMap.put(entry.getKey(), newReplicas);
                
                if (newReplicas.isEmpty()) {
                    System.err.println("[CRITICAL] Partition " + entry.getKey() + " has lost all replicas! Data offline.");
                } else {
                    System.out.printf("[CONTROLLER] Partition %d rebalanced. New Leader: Broker %d\n", 
                        entry.getKey(), newReplicas.get(0));
                }
            }
        }
        
        replenishUnderReplicatedPartitions();
        broadcastClusterMap();
    }

    public boolean replenishUnderReplicatedPartitions() {
        boolean mapChanged = false;

        for (Map.Entry<Integer, List<Integer>> entry : manager.clusterMap.entrySet()) {
            List<Integer> currentReplicas = entry.getValue();
            
            if (currentReplicas.size() < manager.targetReplicationFactor) {
                int needed = manager.targetReplicationFactor - currentReplicas.size();
                
                List<Integer> availableStandbys = new ArrayList<>();
                for (Integer activeBrokerId : manager.activeBrokers.keySet()) {
                    if (!currentReplicas.contains(activeBrokerId)) {
                        availableStandbys.add(activeBrokerId);
                    }
                }

                if (!availableStandbys.isEmpty()) {
                    List<Integer> newReplicas = new ArrayList<>(currentReplicas);
                    
                    for (int i = 0; i < needed && i < availableStandbys.size(); i++) {
                        int chosenStandby = availableStandbys.get(i);
                        newReplicas.add(chosenStandby);
                        System.out.printf("[CONTROLLER] Assigned Standby Broker %d to under-replicated Partition %d\n", chosenStandby, entry.getKey());
                    }
                    
                    manager.clusterMap.put(entry.getKey(), newReplicas);
                    mapChanged = true;
                }
            }
        }
        return mapChanged;
    }

    public void broadcastClusterMap() {
        System.out.println("[CONTROLLER] Broadcasting updated cluster map to surviving workers...");
        
        int bufferSize = 6; 
        for (List<Integer> brokers : manager.clusterMap.values()) {
            bufferSize += 8 + (brokers.size() * 4); 
        }

        ByteBuffer responseBuffer = ByteBuffer.allocate(bufferSize);
        responseBuffer.putShort(RequestCodes.CLUSTER_MAP_UPDATE); 
        responseBuffer.putInt(manager.clusterMap.size());
        
        for (Map.Entry<Integer, List<Integer>> entry : manager.clusterMap.entrySet()) {
            responseBuffer.putInt(entry.getKey());
            responseBuffer.putInt(entry.getValue().size());
            for (int bId : entry.getValue()) {
                responseBuffer.putInt(bId);
            }
        }
        responseBuffer.flip();

        for (Map.Entry<Integer, SocketChannel> entry : manager.workerChannels.entrySet()) {
            try {
                ByteBuffer copy = responseBuffer.duplicate(); 
                while (copy.hasRemaining()) {
                    int bytesWritten = entry.getValue().write(copy);
                    if (bytesWritten == 0) {
                        Thread.sleep(10); 
                    }
                }
            } catch (Exception e) {
                System.err.println("[CONTROLLER] Failed to send map update to Broker " + entry.getKey());
            }
        }
    }

    public void startJanitorThread() {
        Thread janitor = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000); 
                    long now = System.currentTimeMillis();
                    List<Integer> deadBrokers = new ArrayList<>();
                    
                    for (Map.Entry<Integer, Long> entry : manager.lastHeartbeatTimestamp.entrySet()) {
                        if (now - entry.getValue() > 10000) { 
                            deadBrokers.add(entry.getKey());
                        }
                    }
                    
                    for (int deadId : deadBrokers) {
                        System.err.println("\n[JANITOR] 🚨 Broker " + deadId + " IS DEAD (Heartbeat timeout)!");
                        handleBrokerFailure(deadId);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        janitor.setDaemon(true);
        janitor.start();
    }
}