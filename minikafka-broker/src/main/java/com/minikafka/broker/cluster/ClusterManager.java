package com.minikafka.broker.cluster;

import com.minikafka.common.protocol.RequestCodes;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class ClusterManager {
    private final int brokerId;
    private final int port;
    private final String controllerAddress;
    private final boolean isController;
    
    // The Master Map: Partition ID -> List of Broker IDs (Index 0 is Leader)
    private Map<Integer, List<Integer>> clusterMap = new HashMap<>();

    // FIX #2: The Address Book mapping permanent Broker ID -> Current Network Port
    private Map<Integer, Integer> activeBrokers = new HashMap<>();

    public ClusterManager(int brokerId, int port, String controllerAddress) {
        this.brokerId = brokerId;
        this.port = port;
        this.controllerAddress = controllerAddress;
        this.isController = (controllerAddress == null);
    }

    public boolean isController() {
        return isController;
    }

    public void startup(int numPartitions, int numBrokers, int replicationFactor) {
        // FIX #1: Prevent Data Loss Edge Case 
        if (replicationFactor > numBrokers) {
            System.err.println("[FATAL ERROR] Replication factor (" + replicationFactor + ") cannot be greater than total brokers (" + numBrokers + ").");
            // System.exit(1); 
            throw new IllegalArgumentException(
            "[FATAL ERROR] Replication factor (" + replicationFactor + ") cannot be greater than total brokers (" + numBrokers + ")."
        );
        }

        if (isController) {
            System.out.println("[CONTROLLER] Initializing master cluster map...");
            // The Controller must add ITSELF to the address book first
            this.activeBrokers.put(this.brokerId, this.port);
            generateInitialMap(numPartitions, numBrokers, replicationFactor);
        } else {
            System.out.println("[WORKER] Booting up. Connecting to controller at " + controllerAddress + "...");
            registerWithController();
        }
    }

    private void generateInitialMap(int numPartitions, int numBrokers, int replicationFactor) {
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> assignedBrokers = new ArrayList<>();
            int leaderId = partitionId % numBrokers;
            assignedBrokers.add(leaderId);
            
            for (int i = 1; i < replicationFactor; i++) {
                assignedBrokers.add((leaderId + i) % numBrokers);
            }
            
            clusterMap.put(partitionId, assignedBrokers);
            System.out.printf("[CONTROLLER MAP] Partition %d -> Leader: Broker %d | Followers: %s\n", 
                partitionId, leaderId, assignedBrokers.subList(1, assignedBrokers.size()));
        }
    }

    private void registerWithController() {
        try {
            String[] parts = controllerAddress.split(":");
            SocketChannel controllerChannel = SocketChannel.open(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            
            // Keep the channel in BLOCKING mode just for the boot sequence so we can wait for the map
            controllerChannel.configureBlocking(true);
            
            // 1. Send the Registration Request 
            ByteBuffer reqBuffer = ByteBuffer.allocate(10);
            reqBuffer.putShort(RequestCodes.REGISTER_BROKER);
            reqBuffer.putInt(this.brokerId); // Send ID so Controller knows WHO we are
            reqBuffer.putInt(this.port);     // Send Port so Controller knows WHERE we are
            reqBuffer.flip();
            
            while (reqBuffer.hasRemaining()) {
                controllerChannel.write(reqBuffer);
            }
            System.out.println("[WORKER] Registration sent. Awaiting cluster map...");
            
            // 2. Read the Map Response from the Controller
            ByteBuffer respBuffer = ByteBuffer.allocate(1024); // Allocate enough space for the map
            int bytesRead = controllerChannel.read(respBuffer);
            if (bytesRead == -1) throw new IOException("Controller disconnected");
            
            respBuffer.flip();
            
            // 3. Unpack the bytes back into a Java Map
            int partitionsCount = respBuffer.getInt();
            this.clusterMap = new HashMap<>();

            for (int i = 0; i < partitionsCount; i++) {
                int partitionId = respBuffer.getInt();
                int numAssigned = respBuffer.getInt();
                
                List<Integer> brokers = new ArrayList<>();
                for (int j = 0; j < numAssigned; j++) {
                    brokers.add(respBuffer.getInt());
                }
                this.clusterMap.put(partitionId, brokers);
            }

            // 4. Print success to the terminal
            System.out.println("[WORKER] Successfully received cluster map!");
            for (Map.Entry<Integer, List<Integer>> entry : clusterMap.entrySet()) {
                System.out.printf("Partition %d -> Leader: Broker %d | Followers: %s\n", 
                    entry.getKey(), 
                    entry.getValue().get(0), 
                    entry.getValue().subList(1, entry.getValue().size()));
            }

            // Revert back to non-blocking for normal async operations
            controllerChannel.configureBlocking(false);
            
        } catch (IOException e) {
            System.err.println("[WORKER FATAL] Failed to connect to controller: " + e.getMessage());
            System.exit(1); 
        }
    }

    public void handleIncomingWorkerRegistration(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        if (!isController) return;

        int workerId = buffer.getInt();
        int workerPort = buffer.getInt();
        
        // FIX #2: Save or Update the Worker's location in the Address Book
        this.activeBrokers.put(workerId, workerPort);
        
        System.out.printf("[CONTROLLER] Worker Broker %d registered on port %d.\n", workerId, workerPort);
        System.out.println("[CONTROLLER] Sending cluster map back to Broker " + workerId + "...");
        
        // 1. Calculate the exact byte size needed for the buffer
        int bufferSize = 4; // 4 bytes to store the total number of partitions
        for (List<Integer> brokers : clusterMap.values()) {
            bufferSize += 4; // Partition ID
            bufferSize += 4; // Size of broker list
            bufferSize += (brokers.size() * 4); // The actual broker IDs
        }

        // 2. Pack the map into the buffer
        ByteBuffer responseBuffer = ByteBuffer.allocate(bufferSize);
        responseBuffer.putInt(clusterMap.size()); // Total partitions

        for (Map.Entry<Integer, List<Integer>> entry : clusterMap.entrySet()) {
            responseBuffer.putInt(entry.getKey()); // Partition ID
            
            List<Integer> brokers = entry.getValue();
            responseBuffer.putInt(brokers.size()); // How many brokers
            
            for (int bId : brokers) {
                responseBuffer.putInt(bId); // Write each broker ID
            }
        }

        // 3. Send it over the network to the worker
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
            clientChannel.write(responseBuffer);
        }
        System.out.println("[CONTROLLER] Map successfully sent to Broker " + workerId);
    }
}