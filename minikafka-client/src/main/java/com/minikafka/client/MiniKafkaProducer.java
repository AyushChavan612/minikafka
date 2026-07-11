package com.minikafka.client;

import java.io.IOException;

public class MiniKafkaProducer {
    private final HighLevelNetworkClient networkClient;

    public MiniKafkaProducer(String host, int port) throws IOException {
        this.networkClient = new HighLevelNetworkClient(host, port);
    }

    public void send(String topic, String key, byte[] payload) throws IOException {
        networkClient.sendProduceRequest(topic, key, payload);
        System.out.println("Producer successfully sent binary payload to broker.");
    }

    public void close() throws IOException {
        networkClient.close();
    }
}