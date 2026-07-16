package com.minikafka.client;

import java.io.IOException;

public class MiniKafkaConsumer {
    // Holds the network connection
    private final HighLevelNetworkClient networkClient;
    // Holds the polling logic
    private HighLevelConsumer highLevelConsumer;

    public MiniKafkaConsumer(String host, int port) throws IOException {
        // 1. Open the single socket connection
        this.networkClient = new HighLevelNetworkClient(host, port);
    }

    public void subscribe(String groupId, String topic) throws IOException {
        // 2. Connect the files: Pass the socket into the HighLevelConsumer
        this.highLevelConsumer = new HighLevelConsumer(this.networkClient, groupId, topic);
    }

    public void startPolling() throws IOException {
        if (this.highLevelConsumer != null) {
            // 3. Trigger the while(true) loop located in HighLevelConsumer
            this.highLevelConsumer.startContinuousPolling();
        } else {
            System.out.println("ERROR: You must call subscribe() before polling.");
        }
    }

    public void close() throws IOException {
        if (networkClient != null) {
            networkClient.close();
        }
    }
}