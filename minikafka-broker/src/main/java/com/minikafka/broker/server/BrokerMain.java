package com.minikafka.broker.server;

import com.minikafka.broker.network.MiniKafkaServer;
import java.io.IOException;

public class BrokerMain {
    public static void main(String[] args) {
        MiniKafkaServer server = new MiniKafkaServer(9092);
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}