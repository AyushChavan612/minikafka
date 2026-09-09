package com.minikafka.broker.cluster;

import com.minikafka.common.protocol.RequestCodes;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class WorkerService {
    private final ClusterManager manager;
    private SocketChannel controllerChannel;

    public WorkerService(ClusterManager manager) {
        this.manager = manager;
    }

    public void registerWithController() {
        try {
            String[] parts = manager.controllerAddress.split(":");
            this.controllerChannel = SocketChannel.open(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            this.controllerChannel.configureBlocking(true);
            
            ByteBuffer reqBuffer = ByteBuffer.allocate(10);
            reqBuffer.putShort(RequestCodes.REGISTER_BROKER);
            reqBuffer.putInt(manager.brokerId); 
            reqBuffer.putInt(manager.port);     
            reqBuffer.flip();
            
            while (reqBuffer.hasRemaining()) {
                controllerChannel.write(reqBuffer);
            }
            System.out.println("[WORKER] Registration sent. Awaiting cluster map...");
            
            ByteBuffer respBuffer = ByteBuffer.allocate(1024); 
            int bytesRead = controllerChannel.read(respBuffer);
            if (bytesRead == -1) throw new IOException("Controller disconnected");
            
            respBuffer.flip();
            
            int partitionsCount = respBuffer.getInt();
            Map<Integer, List<Integer>> newMap = new HashMap<>();

            for (int i = 0; i < partitionsCount; i++) {
                int partitionId = respBuffer.getInt();
                int numAssigned = respBuffer.getInt();
                
                List<Integer> brokers = new ArrayList<>();
                for (int j = 0; j < numAssigned; j++) {
                    brokers.add(respBuffer.getInt());
                }
                newMap.put(partitionId, brokers);
            }

            manager.clusterMap = newMap;

            System.out.println("[WORKER] Successfully received cluster map!");
            for (Map.Entry<Integer, List<Integer>> entry : manager.clusterMap.entrySet()) {
                System.out.printf("Partition %d -> Leader: Broker %d | Followers: %s\n", 
                    entry.getKey(), 
                    entry.getValue().get(0), 
                    entry.getValue().subList(1, entry.getValue().size()));
            }

            startHeartbeatThread();
            startControllerListenerThread();
        } catch (IOException e) {
            System.err.println("[WORKER FATAL] Failed to connect to controller: " + e.getMessage());
            System.exit(1); 
        }
    }

    public void handleDynamicMapUpdate(ByteBuffer buffer) {
        System.out.println("\n[WORKER] Received dynamic cluster map update from Controller!");
        
        int partitionsCount = buffer.getInt();
        Map<Integer, List<Integer>> newMap = new HashMap<>();

        for (int i = 0; i < partitionsCount; i++) {
            int partitionId = buffer.getInt();
            int numAssigned = buffer.getInt();
            
            List<Integer> brokers = new ArrayList<>();
            for (int j = 0; j < numAssigned; j++) {
                brokers.add(buffer.getInt());
            }
            newMap.put(partitionId, brokers);
        }

        manager.clusterMap = newMap;

        for (Map.Entry<Integer, List<Integer>> entry : manager.clusterMap.entrySet()) {
            System.out.printf("Partition %d -> Leader: Broker %d | Followers: %s\n", 
                entry.getKey(), 
                entry.getValue().get(0), 
                entry.getValue().subList(1, entry.getValue().size()));
        }
    }

    private void startHeartbeatThread() {
        Thread heartbeatThread = new Thread(() -> {
            ByteBuffer pingBuffer = ByteBuffer.allocate(6);
            while (true) {
                try {
                    Thread.sleep(3000); 
                    pingBuffer.clear();
                    pingBuffer.putShort(RequestCodes.BROKER_HEARTBEAT);
                    pingBuffer.putInt(manager.brokerId); 
                    pingBuffer.flip();
                    
                    while (pingBuffer.hasRemaining()) {
                        this.controllerChannel.write(pingBuffer);
                    }
                } catch (IOException e) {
                    System.err.println("\n[WORKER FATAL] Lost connection to Controller! " + e.getMessage());
                    System.exit(1);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true); 
        heartbeatThread.start();
    }

    private void startControllerListenerThread() {
        Thread listenerThread = new Thread(() -> {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            while (true) {
                try {
                    buffer.clear();
                    int bytesRead = this.controllerChannel.read(buffer); 
                    
                    if (bytesRead == -1) {
                        System.err.println("\n[WORKER FATAL] Controller disconnected!");
                        System.exit(1);
                    }
                    buffer.flip();
                    
                    if (buffer.remaining() >= 2) {
                        short requestCode = buffer.getShort();
                        if (requestCode == RequestCodes.CLUSTER_MAP_UPDATE) {
                            handleDynamicMapUpdate(buffer);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("\n[WORKER FATAL] Connection to Controller broken: " + e.getMessage());
                    System.exit(1);
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }
}