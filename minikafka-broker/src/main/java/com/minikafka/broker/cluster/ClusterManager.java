package com.minikafka.broker.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;

@Data
public class ClusterManager {
    
    // Core Data Structures (Package-private so Services can access them)
    volatile Map<Integer, List<Integer>> clusterMap = new HashMap<>();
    Map<Integer, Integer> activeBrokers = new ConcurrentHashMap<>();
    Map<Integer, Long> lastHeartbeatTimestamp = new ConcurrentHashMap<>();
    Map<Integer, SocketChannel> workerChannels = new ConcurrentHashMap<>();

    // Configuration
    final int brokerId;
    final int port;
    final String controllerAddress;
    final boolean isController;
    int targetReplicationFactor;
    
    // Extracted Logic Services
    private final ControllerService controllerService;
    private final WorkerService workerService;

    public ClusterManager(int brokerId, int port, String controllerAddress) {
        this.brokerId = brokerId;
        this.port = port;
        this.controllerAddress = controllerAddress;
        this.isController = (controllerAddress == null);
        
        // Initialize only the service we need
        if (this.isController) {
            this.controllerService = new ControllerService(this);
            this.workerService = null;
        } else {
            this.controllerService = null;
            this.workerService = new WorkerService(this);
        }
    }

    public boolean isController() {
        return isController;
    }

    public void startup(int numPartitions, int numBrokers, int replicationFactor) {
        if (replicationFactor > numBrokers) {
            throw new IllegalArgumentException(
                "[FATAL ERROR] Replication factor (" + replicationFactor + ") cannot be greater than total brokers (" + numBrokers + ")."
            );
        }

        this.targetReplicationFactor = replicationFactor;

        if (isController) {
            System.out.println("[CONTROLLER] Initializing master cluster map...");
            this.activeBrokers.put(this.brokerId, this.port);
            
            controllerService.generateInitialMap(numPartitions, numBrokers, replicationFactor);
            controllerService.startJanitorThread();
        } else {
            System.out.println("[WORKER] Booting up. Connecting to controller at " + controllerAddress + "...");
            workerService.registerWithController();
        }
    }

    // ==========================================
    // Public API for MiniKafkaServer to call
    // ==========================================

    public void handleIncomingWorkerRegistration(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        if (isController) controllerService.handleIncomingWorkerRegistration(buffer, clientChannel);
    }

    public void handleBrokerHeartbeat(ByteBuffer buffer) {
        if (isController) controllerService.handleBrokerHeartbeat(buffer);
    }

    public void handleDynamicMapUpdate(ByteBuffer buffer) {
        if (!isController) workerService.handleDynamicMapUpdate(buffer);
    }
    
    // ==========================================
    // Pass-throughs to keep JUnit Tests working
    // ==========================================

    @SuppressWarnings("unused") // Called via Java Reflection in JUnit tests
    private void handleBrokerFailure(int deadBrokerId) {
        if (isController) controllerService.handleBrokerFailure(deadBrokerId);
    }

    @SuppressWarnings("unused") // Called via Java Reflection in JUnit tests
    private boolean replenishUnderReplicatedPartitions() {
        if (isController) return controllerService.replenishUnderReplicatedPartitions();
        return false;
    }
}