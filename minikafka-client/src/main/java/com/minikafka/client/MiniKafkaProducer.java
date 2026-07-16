package com.minikafka.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.minikafka.model.RecordBatch;
import com.minikafka.common.utils.DefaultPartitioner;
import com.minikafka.common.protocol.NumberOfPartitions;

public class MiniKafkaProducer {

    private final HighLevelNetworkClient networkClient;
    // --- BATCHING CONFIGURATION ---
    private final int BATCH_SIZE_BYTES = 16384; // 16 KB
    private final long LINGER_MS = 50;          // 50 ms time limit
    
    // Structure: Topic -> (PartitionID -> RecordBatch)
    private final Map<String, Map<Integer, RecordBatch>> accumulator;
    private volatile boolean isRunning = true;
    private final Thread senderThread;

    public MiniKafkaProducer(String host, int port) throws IOException {
        this.networkClient = new HighLevelNetworkClient(host, port);
        this.accumulator = new ConcurrentHashMap<>();

        // Start the background I/O network thread
        this.senderThread = new Thread(this::runSenderLoop);
        this.senderThread.setDaemon(true);
        this.senderThread.start();
    }

    /**
     * MAIN THREAD: Fast, non-blocking memory write
     */
    public void send(String topic, String key, byte[] payload) {
        // Assume 3 partitions for now; this routes the key to a specific partition
        int partitionId = DefaultPartitioner.partition(key, NumberOfPartitions.getNumberOfPartitions()); 

        Map<Integer, RecordBatch> topicBatches = accumulator.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
        
        synchronized (topicBatches) {
            RecordBatch batch = topicBatches.computeIfAbsent(partitionId, k -> new RecordBatch());
            batch.add(key, payload);
        }
    }

    /**
     * BACKGROUND THREAD: Constantly evaluates the Linger (Time) and Batch Size limits.
     */
    private void runSenderLoop() {
        while (isRunning) {
            try {
                long now = System.currentTimeMillis();
                
                for (String topic : accumulator.keySet()) {
                    Map<Integer, RecordBatch> topicBatches = accumulator.get(topic);
                    
                    for (Integer partitionId : topicBatches.keySet()) {
                        RecordBatch batch;
                        
                        synchronized (topicBatches) {
                            batch = topicBatches.get(partitionId);
                            if (batch == null || batch.getPayloads().isEmpty()) continue;

                            boolean isFull = batch.getTotalBytes() >= BATCH_SIZE_BYTES;
                            boolean isExpired = (now - batch.getCreatedAt()) >= LINGER_MS;

                            if (isFull || isExpired) {
                                // Remove from map so the Main Thread creates a fresh batch next time
                                topicBatches.remove(partitionId); 
                            } else {
                                batch = null; // Don't send yet
                            }
                        }

                        // Do the network I/O outside the synchronized block!
                        if (batch != null) {
                            flushBatchToNetwork(topic, partitionId, batch);
                        }
                    }
                }
                
                Thread.sleep(5); // Prevent CPU burning
            } catch (Exception e) {
                if (isRunning) System.err.println("Sender Thread Error: " + e.getMessage());
            }
        }
    }

    private void flushBatchToNetwork(String topic, int partitionId, RecordBatch batch) throws IOException {
        System.out.printf("[PRODUCER] Flushing Batch to %s-%d | Messages: %d | Size: %d bytes%n", 
                topic, partitionId, batch.getPayloads().size(), batch.getTotalBytes());
        
        // This is the NEW method we will write next in HighLevelNetworkClient!
        networkClient.sendProduceBatchRequest(topic, partitionId, batch);
    }

    public void close() throws IOException {
        isRunning = false;
        try {
            senderThread.join(1000); // Wait for the sender thread to cleanly finish flushing
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        networkClient.close();
    }
}