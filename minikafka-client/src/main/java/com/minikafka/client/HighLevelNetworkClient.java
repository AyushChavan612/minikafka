package com.minikafka.client;

import com.minikafka.common.protocol.RequestCodes;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class HighLevelNetworkClient {
    private final SocketChannel socketChannel;

    public HighLevelNetworkClient(String host, int port) throws IOException {
        this.socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    // --- PRODUCER API ---
    public void sendProduceRequest(String topic, String key, byte[] payload) throws IOException {
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
    }

    // --- FETCH DATA API (Used by both consumers) ---
    public String sendFetchRequest(String topic, int partitionId, long offset) throws IOException {
        byte[] topicBytes = topic != null ? topic.getBytes() : new byte[0];
        ByteBuffer requestBuffer = ByteBuffer.allocate(18 + topicBytes.length);

        requestBuffer.putShort(RequestCodes.FETCH);
        requestBuffer.putInt(topicBytes.length);
        if (topicBytes.length > 0) requestBuffer.put(topicBytes);

        requestBuffer.putInt(partitionId);
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
            
            ByteBuffer payloadBuffer = ByteBuffer.allocate(lenBuffer.getInt());
            socketChannel.read(payloadBuffer);
            payloadBuffer.flip();

            return new String(payloadBuffer.array());
        }
        return null;
    }

    // --- OFFSET MANAGEMENT APIs ---
    public long fetchOffset(String groupId, String topic, int partitionId) throws IOException {
        byte[] groupBytes = groupId.getBytes();
        byte[] topicBytes = topic.getBytes();

        ByteBuffer request = ByteBuffer.allocate(14 + groupBytes.length + topicBytes.length);
        request.putShort(RequestCodes.FETCH_OFFSET);
        request.putInt(groupBytes.length);
        request.put(groupBytes);
        request.putInt(topicBytes.length);
        request.put(topicBytes);
        request.putInt(partitionId);

        request.flip();
        while (request.hasRemaining()) {
            socketChannel.write(request);
        }

        ByteBuffer response = ByteBuffer.allocate(8);
        socketChannel.read(response);
        response.flip();
        return response.getLong();
    }

    public void commitOffset(String groupId, String topic, int partitionId, long offsetToCommit) throws IOException {
        byte[] groupBytes = groupId.getBytes();
        byte[] topicBytes = topic.getBytes();

        ByteBuffer request = ByteBuffer.allocate(22 + groupBytes.length + topicBytes.length);
        request.putShort(RequestCodes.COMMIT_OFFSET);
        request.putInt(groupBytes.length);
        request.put(groupBytes);
        request.putInt(topicBytes.length);
        request.put(topicBytes);
        request.putInt(partitionId);
        request.putLong(offsetToCommit);

        request.flip();
        while (request.hasRemaining()) {
            socketChannel.write(request);
        }

        ByteBuffer response = ByteBuffer.allocate(1);
        socketChannel.read(response);
        response.flip();

        // ERRROR HANDLING: Generate error if response is 0
        if (response.hasRemaining()) {
            byte status = response.get();
            if (status == 0) {
                throw new IOException("CRITICAL: Failed to commit offset " + offsetToCommit + ". Broker rejected the request.");
            }
        }
    }

    public int joinGroup(String topic, String groupId, String consumerId) throws IOException {
        byte[] topicBytes = topic.getBytes();
        byte[] groupBytes = groupId.getBytes();
        byte[] consumerBytes = consumerId.getBytes();

        ByteBuffer request = ByteBuffer.allocate(14 + topicBytes.length + groupBytes.length + consumerBytes.length);
        request.putShort((short) 5); // JOIN_GROUP API
        request.putInt(topicBytes.length);
        request.put(topicBytes);
        request.putInt(groupBytes.length);
        request.put(groupBytes);
        request.putInt(consumerBytes.length);
        request.put(consumerBytes);

        request.flip();
        while (request.hasRemaining()) {
            socketChannel.write(request);
        }

        ByteBuffer response = ByteBuffer.allocate(4);
        socketChannel.read(response);
        response.flip();
        return response.getInt();
    }

    public void close() throws IOException {
        if (socketChannel != null && socketChannel.isOpen()) {
            socketChannel.close();
        }
    }
}