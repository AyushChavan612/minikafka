package com.minikafka.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.minikafka.common.protocol.RequestCodes;

public class MiniKafkaProducer {
    
    private final SocketChannel socketChannel;

    public MiniKafkaProducer(String host, int port) throws IOException {
        this.socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    public void send(String topic, String key, byte[] payload) throws IOException {
        byte[] topicBytes = topic != null ? topic.getBytes() : new byte[0];
        byte[] keyBytes = key != null ? key.getBytes() : new byte[0];

        int totalSize = 14 + topicBytes.length + keyBytes.length + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        buffer.putShort(RequestCodes.PRODUCE);
        buffer.putInt(topicBytes.length);
        if (topicBytes.length > 0) buffer.put(topicBytes);

        buffer.putInt(keyBytes.length);
        if (keyBytes.length > 0) buffer.put(keyBytes);

        buffer.putInt(payload.length);
        if (payload.length > 0) buffer.put(payload);

        buffer.flip();
        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
        
        System.out.println("Producer successfully sent binary payload to broker.");
    }

    public void close() throws IOException {
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
    }
}