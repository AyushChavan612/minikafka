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
import com.minikafka.common.protocol.RequestCodes;
import com.minikafka.broker.consumer.GroupCoordinator;
import com.minikafka.broker.storage.TopicManager;
import com.minikafka.common.model.LogRecord;

public class MiniKafkaServer {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private boolean isRunning;
    private final TopicManager topicManager;
    private final GroupCoordinator groupCoordinator;
    private final int numPartitions;

    public MiniKafkaServer(int port, int numPartitions) {
        this.port = port;
        this.numPartitions = numPartitions;
        this.topicManager = new TopicManager(numPartitions);
        this.groupCoordinator = new GroupCoordinator();
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

    private boolean readFully(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = clientChannel.read(buffer);
            if (bytesRead == -1) {
                return false; // Client disconnected
            }
            if (bytesRead == 0) {
                // Wait 1ms for the rest of the chopped up TCP packets to arrive
                try {
                    Thread.sleep(1); 
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return true;
    }

    private void readClientData(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();

        try {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            if (!readFully(clientChannel, sizeBuffer)) {
                System.out.println("Disconnected: " + clientChannel.getRemoteAddress());
                clientChannel.close();
                key.cancel();
                return;
            }

            sizeBuffer.flip();
            int messageSize = sizeBuffer.getInt();

            // Safety Check for bad data
            if (messageSize <= 0 || messageSize > 10 * 1024 * 1024) { 
                System.err.println("Invalid message size received: " + messageSize);
                clientChannel.close();
                key.cancel();
                return;
            }

            // 2. Allocate exact memory for the Payload and wait for everything!
            ByteBuffer payloadBuffer = ByteBuffer.allocate(messageSize);
            if (!readFully(clientChannel, payloadBuffer)) {
                System.out.println("Disconnected during payload collection: " + clientChannel.getRemoteAddress());
                clientChannel.close();
                key.cancel();
                return;
            }

            payloadBuffer.flip();

            // 3. Now we safely unpack the API key and route it
            short apiKey = payloadBuffer.getShort();
            switch (apiKey) {
                case RequestCodes.PRODUCE:
                    handleProduceRequest(payloadBuffer);
                    break;
                case RequestCodes.PRODUCE_BATCH:             
                    handleProduceBatchRequest(payloadBuffer);        
                    break;
                case RequestCodes.FETCH:
                    handleFetchRequest(payloadBuffer, clientChannel);
                    break;
                case RequestCodes.FETCH_OFFSET:
                    handleFetchOffsetRequest(payloadBuffer, clientChannel);
                    break;
                case RequestCodes.COMMIT_OFFSET:
                    handleCommitOffsetRequest(payloadBuffer, clientChannel);
                    break;
                case RequestCodes.JOIN_GROUP:
                    handleJoinGroupRequest(payloadBuffer, clientChannel);
                    break;
                case RequestCodes.HEARTBEAT:                 
                    handleHeartbeatRequest(payloadBuffer);        
                    break;
                default:
                    System.err.println("Unknown API Key: " + apiKey);
            }

        } catch (Exception e) {
            System.err.println("Failed to process request: " + e.getMessage());
            e.printStackTrace();
            clientChannel.close();
            key.cancel();
        }
    }

    // ===================== API REQUEST HANDLERS ==========================

    private void handleProduceRequest(ByteBuffer buffer) throws IOException {
        DataDecoder.DecodedRecord record = DataDecoder.decode(buffer);
        System.out.println("--- INCOMING PRODUCE REQUEST ---");
        System.out.println("Topic: " + record.topic);
        topicManager.routeRecord(record.topic, record.key, record.payload);
    }

    private void handleProduceBatchRequest(ByteBuffer buffer) {
        System.out.println("\n--- INCOMING PRODUCE BATCH REQUEST ---");

        // 1. Read Topic
        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        // 2. Read Routing Info
        int partitionId = buffer.getInt();
        int recordCount = buffer.getInt();

        System.out.printf("Topic: %s | Partition: %d | Batch Size: %d messages%n", topic, partitionId, recordCount);

        // 3. Loop through the batch and unpack every message!
        for (int i = 0; i < recordCount; i++) {
            
            // Unpack Key
            int keyLen = buffer.getInt();
            String key = null;
            if (keyLen > 0) {
                byte[] keyBytes = new byte[keyLen];
                buffer.get(keyBytes);
                key = new String(keyBytes);
            }

            // Unpack Payload
            int payloadLen = buffer.getInt();
            byte[] payloadBytes = new byte[payloadLen];
            buffer.get(payloadBytes);
            String payload = new String(payloadBytes);

            // 4. Send directly to the disk without recalculating the partition!
            topicManager.appendToPartition(topic, partitionId, key, payload);
        }
        System.out.println("--- BATCH SUCCESSFULLY WRITTEN TO DISK ---");
    }

    private void handleFetchRequest(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        System.out.println("--- INCOMING FETCH REQUEST ---");

        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();

        System.out.println("Requested Topic: " + topic + " | Partition: " + partitionId + " | Offset: " + offset);

        LogRecord record = topicManager.fetchRecord(topic, partitionId, offset);

        ByteBuffer responseBuffer;
        if (record != null) {
            byte[] payload = record.getPayload();
            responseBuffer = ByteBuffer.allocate(1 + 4 + payload.length);
            responseBuffer.put((byte) 1); // Status 1 = SUCCESS
            responseBuffer.putInt(payload.length);
            responseBuffer.put(payload);
        } else {
            responseBuffer = ByteBuffer.allocate(1);
            responseBuffer.put((byte) 0); // Status 0 = NOT FOUND
        }

        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
            clientChannel.write(responseBuffer);
        }
    }

    private void handleFetchOffsetRequest(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        int groupIdLen = buffer.getInt();
        byte[] groupIdBytes = new byte[groupIdLen];
        buffer.get(groupIdBytes);
        String groupId = new String(groupIdBytes);

        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();

        long currentOffset = groupCoordinator.fetchOffset(groupId, topic, partitionId);

        ByteBuffer responseBuffer = ByteBuffer.allocate(8);
        responseBuffer.putLong(currentOffset);
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
            clientChannel.write(responseBuffer);
        }
    }

    private void handleCommitOffsetRequest(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        int groupIdLen = buffer.getInt();
        byte[] groupIdBytes = new byte[groupIdLen];
        buffer.get(groupIdBytes);
        String groupId = new String(groupIdBytes);

        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();

        groupCoordinator.commitOffset(groupId, topic, partitionId, offset);

        ByteBuffer responseBuffer = ByteBuffer.allocate(1);
        responseBuffer.put((byte) 1);
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
            clientChannel.write(responseBuffer);
        }
    }

    private void handleJoinGroupRequest(ByteBuffer buffer, SocketChannel clientChannel) throws IOException {
        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int groupLen = buffer.getInt();
        byte[] groupBytes = new byte[groupLen];
        buffer.get(groupBytes);
        String groupId = new String(groupBytes);

        int consumerIdLen = buffer.getInt();
        byte[] consumerIdBytes = new byte[consumerIdLen];
        buffer.get(consumerIdBytes);
        String consumerId = new String(consumerIdBytes);

        int assignedPartition = groupCoordinator.registerConsumer(groupId, topic, consumerId, 3);

        ByteBuffer responseBuffer = ByteBuffer.allocate(4);
        responseBuffer.putInt(assignedPartition);
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
            clientChannel.write(responseBuffer);
        }
    }

    private void handleHeartbeatRequest(ByteBuffer buffer) {
        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int groupLen = buffer.getInt();
        byte[] groupBytes = new byte[groupLen];
        buffer.get(groupBytes);
        String groupId = new String(groupBytes);

        int consumerIdLen = buffer.getInt();
        byte[] consumerIdBytes = new byte[consumerIdLen];
        buffer.get(consumerIdBytes);
        String consumerId = new String(consumerIdBytes);

        // Tell the coordinator this consumer is still alive!
        groupCoordinator.recordHeartbeat(groupId, topic, consumerId);
    }

    public void stop() throws IOException {
        isRunning = false;
        selector.wakeup();
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
        }
    }
}
