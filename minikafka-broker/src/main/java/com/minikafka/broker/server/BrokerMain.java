package com.minikafka.broker.server;

import com.minikafka.broker.network.MiniKafkaServer;
import java.io.IOException;

public class BrokerMain {
    public static void main(String[] args) {
        // Enforce the new required terminal inputs
        if (args.length < 5) {
            System.out.println("Usage for Controller: java BrokerMain <brokerId> <port> <numPartitions> <numBrokers> <repFactor>");
            System.out.println("Usage for Worker:     java BrokerMain <brokerId> <port> <numPartitions> <numBrokers> <repFactor> <controllerHost:port>");
            System.exit(1);
        }

        int brokerId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        int numPartitions = Integer.parseInt(args[2]);
        int numBrokers = Integer.parseInt(args[3]);
        int repFactor = Integer.parseInt(args[4]);
        
        String controllerAddress = null;

        // If a 6th argument is passed, this is a Worker
        if (args.length >= 6) {
            controllerAddress = args[5];
            System.out.printf("[BOOT] Worker %d (Port: %d). Cluster config: %d Partitions, %d Brokers, RF=%d. Controller at %s%n", 
                brokerId, port, numPartitions, numBrokers, repFactor, controllerAddress);
        } else {
            System.out.printf("[BOOT] CONTROLLER %d (Port: %d). Generating map for: %d Partitions, %d Brokers, RF=%d.%n", 
                brokerId, port, numPartitions, numBrokers, repFactor);
        }

        MiniKafkaServer server = new MiniKafkaServer(brokerId, port, numPartitions, numBrokers, repFactor, controllerAddress);
        
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}