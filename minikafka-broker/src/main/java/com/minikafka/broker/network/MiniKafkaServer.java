package com.minikafka.broker.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import com.minikafka.common.protocol.DataDecoder;
import com.minikafka.broker.storage.TopicManager;

public class MiniKafkaServer {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private boolean isRunning;
    private final TopicManager topicManager;

    public MiniKafkaServer(int port) {
        this.port = port;
        this.topicManager = new TopicManager(3);
    }

    public void start() throws IOException {
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        isRunning = true;

        System.out.println("Broker listening on port " + port);

        while (isRunning) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) {
                    continue;
                }

                if (key.isAcceptable()) {
                    acceptClient(key);
                } else if (key.isReadable()) {
                    readClientData(key);
                }
            }
        }
    }

    private void acceptClient(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
        System.out.println("Connected: " + clientChannel.getRemoteAddress());
    }

    private void readClientData(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int bytesRead = clientChannel.read(buffer);

        if (bytesRead == -1) {
            System.out.println("Disconnected: " + clientChannel.getRemoteAddress());
            clientChannel.close();
            key.cancel();
            return;
        }

        buffer.flip();

        try {
            DataDecoder.DecodedRecord record = DataDecoder.decode(buffer);

            System.out.println("--- INCOMING EVENT DECODED ---");
            System.out.println("Topic:   " + record.topic);
            System.out.println("Key:     " + record.key);
            System.out.println("Payload: " + record.payload);
            System.out.println("------------------------------");
            topicManager.routeRecord(record.topic, record.key, record.payload);

        } catch (Exception e) {
            System.err.println("Failed to decode binary payload: " + e.getMessage());
            buffer.clear();
        }
    }

    public void stop() throws IOException {
        isRunning = false;
        selector.wakeup();
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
        }
    }
}