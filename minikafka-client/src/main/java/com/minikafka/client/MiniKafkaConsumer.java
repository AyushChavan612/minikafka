package com.minikafka.client;

import java.io.IOException;

public class MiniKafkaConsumer {
    private final HighLevelNetworkClient networkClient;

    public MiniKafkaConsumer(String host, int port) throws IOException {
        this.networkClient = new HighLevelNetworkClient(host, port);
    }

    public void fetch(String topic, int partitionId, long offset) throws IOException {
        String payload = networkClient.sendFetchRequest(topic, partitionId, offset);
        
        if (payload != null) {
            System.out.println("CONSUMED DATA: " + payload);
        } else {
            System.out.println("No data found at offset " + offset);
        }
    }

    public void close() throws IOException {
        networkClient.close();
    }
}