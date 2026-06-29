package com.minikafka.client;

import com.minikafka.common.protocol.RequestCodes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MiniKafkaConsumer {
    private final SocketChannel socketChannel;

    public MiniKafkaConsumer(String host, int port) throws IOException {
        this.socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    public void fetch(String topic, int partitionId, long offset) throws IOException {
        byte[] topicBytes = topic != null ? topic.getBytes() : new byte[0];

        // Size: 2 (apiKey) + 4 (topicLen) + topic + 4 (partitionId) + 8 (offset)
        int bufferSize = 18 + topicBytes.length;
        ByteBuffer requestBuffer = ByteBuffer.allocate(bufferSize);

        requestBuffer.putShort(RequestCodes.FETCH);
        requestBuffer.putInt(topicBytes.length);
        if (topicBytes.length > 0)
            requestBuffer.put(topicBytes);

        requestBuffer.putInt(partitionId); // Add Partition ID to the request!
        requestBuffer.putLong(offset);

        requestBuffer.flip();
        while (requestBuffer.hasRemaining()) {
            socketChannel.write(requestBuffer);
        }

        ByteBuffer statusBuffer = ByteBuffer.allocate(1);
        socketChannel.read(statusBuffer);
        statusBuffer.flip();

        if (statusBuffer.hasRemaining() && statusBuffer.get() == 1) {
            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
            socketChannel.read(lenBuffer);
            lenBuffer.flip();
            int payloadLen = lenBuffer.getInt();

            ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLen);
            socketChannel.read(payloadBuffer);
            payloadBuffer.flip();

            String payload = new String(payloadBuffer.array());
            System.out.println("CONSUMED DATA: " + payload);
        } else {
            System.out.println("No data found at offset " + offset);
        }
    }

    public void close() throws IOException {
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
    }
}