package com.minikafka.client;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterLoadTest {

    public static void main(String[] args) throws Exception {
        System.out.println("==========================================");
        System.out.println("🚀 STARTING MINIKAFKA DATA INTEGRITY TEST");
        System.out.println("==========================================");

        String topic = "integrity-test-topic";
        String groupId = "integrity-test-group";
        int controllerPort = 9092; 

        int numProducers = 5;
        int messagesPerProducer = 20; 
        int numConsumers = 3; 

        // Thread-safe set to track exact payloads
        Set<String> expectedMessages = ConcurrentHashMap.newKeySet();
        
        AtomicInteger totalSent = new AtomicInteger(0);
        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger unexpectedOrDuplicate = new AtomicInteger(0);
        
        CountDownLatch producersDone = new CountDownLatch(numProducers);

        // ---------------------------------------------------------
        // 1. START PRODUCERS
        // ---------------------------------------------------------
        System.out.println("\n[SYSTEM] Booting up " + numProducers + " Producers...");
        
        for (int i = 0; i < numProducers; i++) {
            final int producerId = i;
            new Thread(() -> {
                try {
                    MiniKafkaProducer producer = new MiniKafkaProducer("localhost", controllerPort);
                    for (int m = 0; m < messagesPerProducer; m++) {
                        String key = "sensor-" + producerId; 
                        
                        // Generate a unique, deterministic payload
                        String payload = "IntegrityCheck | Producer:" + producerId + " | MsgSeq:" + m;
                        
                        // Store it in our global tracker BEFORE sending
                        expectedMessages.add(payload);
                        
                        producer.send(topic, key, payload.getBytes());
                        totalSent.incrementAndGet();
                        
                        Thread.sleep(50); 
                    }
                    producer.close();
                } catch (Exception e) {
                    System.err.println("Producer " + producerId + " crashed: " + e.getMessage());
                } finally {
                    producersDone.countDown();
                }
            }).start();
        }

        producersDone.await();
        System.out.println("\n[SYSTEM] ✅ All Producers finished. Total messages sent: " + totalSent.get());
        System.out.println("[SYSTEM] Giving cluster 2 seconds to flush to disk...\n");
        Thread.sleep(2000);

        // ---------------------------------------------------------
        // 2. START CONSUMERS
        // ---------------------------------------------------------
        System.out.println("==========================================");
        System.out.println("🎧 STARTING CONSUMER GROUP READ");
        System.out.println("==========================================");

        for (int i = 0; i < numConsumers; i++) {
            final int consumerId = i;
            new Thread(() -> {
                try {
                    HighLevelNetworkClient adminClient = new HighLevelNetworkClient("localhost", controllerPort);
                    HighLevelConsumer consumer = new HighLevelConsumer(adminClient, groupId, topic);

                    long endTime = System.currentTimeMillis() + 6000;
                    while (System.currentTimeMillis() < endTime) {
                        String data = consumer.poll();
                        if (data != null) {
                            // Cross-reference the payload with our global tracker
                            boolean wasExpected = expectedMessages.remove(data);
                            
                            if (wasExpected) {
                                totalReceived.incrementAndGet();
                                System.out.printf("Consumer-%d verified: %s%n", consumerId, data);
                            } else {
                                unexpectedOrDuplicate.incrementAndGet();
                                System.err.printf("❌ DATA ERROR! Consumer-%d read unknown/duplicate: %s%n", consumerId, data);
                            }
                        } else {
                            Thread.sleep(100);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Consumer " + consumerId + " crashed: " + e.getMessage());
                }
            }).start();
        }

        Thread.sleep(7000);
        
        System.out.println("\n==========================================");
        System.out.println("📊 DATA INTEGRITY RESULTS");
        System.out.println("==========================================");
        System.out.println("Total Sent:             " + totalSent.get());
        System.out.println("Total Received (Valid): " + totalReceived.get());
        System.out.println("Missing Messages:       " + expectedMessages.size());
        System.out.println("Invalid/Duplicates:     " + unexpectedOrDuplicate.get());
        
        if (expectedMessages.isEmpty() && unexpectedOrDuplicate.get() == 0 && totalSent.get() == totalReceived.get()) {
            System.out.println("\n✅ SUCCESS: Flawless Data Integrity! 0 bytes lost, 0 duplicates.");
        } else {
            System.out.println("\n❌ FAILURE: Cluster dropped or corrupted messages.");
            for (String missing : expectedMessages) {
                System.out.println("   -> MISSING: " + missing);
            }
        }
        
        System.exit(0);
    }
}