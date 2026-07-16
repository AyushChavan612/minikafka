package com.minikafka.broker.server;

import com.minikafka.broker.network.MiniKafkaServer;
import java.io.IOException;
import com.minikafka.common.protocol.NumberOfPartitions;

public class BrokerMain {
    public static void main(String[] args) {
        
        NumberOfPartitions.setNumberOfPartition(3);
        MiniKafkaServer server = new MiniKafkaServer(9092,NumberOfPartitions.getNumberOfPartitions());
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
