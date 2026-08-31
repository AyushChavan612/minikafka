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
import com.minikafka.broker.cluster.ClusterManager;
import com.minikafka.broker.consumer.GroupCoordinator;
import com.minikafka.broker.storage.TopicManager;
import com.minikafka.common.model.LogRecord;

public class MiniKafkaServer {

    private final int port;
    private final int numPartitions;
    private final int numBrokers;
    private final int replicationFactor;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private boolean isRunning;
    private final TopicManager topicManager;
    private final GroupCoordinator groupCoordinator;
    private final ClusterManager clusterManager;

  public MiniKafkaServer(int brokerId, int port, int numPartitions, int numBrokers, int replicationFactor, String controllerAddress) {
        this.port = port;
        this.numPartitions = numPartitions;
        this.numBrokers = numBrokers;
        this.replicationFactor = replicationFactor;
        this.topicManager = new TopicManager(numPartitions); 
        this.groupCoordinator = new GroupCoordinator();
        this.clusterManager = new ClusterManager(brokerId, port, controllerAddress);
    }

    public void start() throws IOException {
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        isRunning = true;

        System.out.println("Broker listening on port " + port);
      clusterManager.startup(numPartitions, numBrokers, replicationFactor);

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
            short apiKey = buffer.getShort();
            switch (apiKey) {
                case RequestCodes.PRODUCE:
                    handleProduceRequest(buffer);
                    break;
                case RequestCodes.FETCH:
                    handleFetchRequest(buffer, clientChannel);
                    break;
                case RequestCodes.FETCH_OFFSET:
                    handleFetchOffsetRequest(buffer, clientChannel);
                    break;
                case RequestCodes.COMMIT_OFFSET:
                    handleCommitOffsetRequest(buffer, clientChannel);
                    break;
                case RequestCodes.JOIN_GROUP:
                    handleJoinGroupRequest(buffer, clientChannel);
                    break;
                case RequestCodes.HEARTBEAT:                 
                    handleHeartbeatRequest(buffer);       
                    break;
                case RequestCodes.REGISTER_BROKER: 
                    clusterManager.handleIncomingWorkerRegistration(buffer, clientChannel);
                    break;
                // Route Broker Heartbeats to Controller
                case RequestCodes.BROKER_HEARTBEAT:
                    clusterManager.handleBrokerHeartbeat(buffer);
                    break;
            // Route Dynamic Map Updates to Worker
                case RequestCodes.CLUSTER_MAP_UPDATE:
                    clusterManager.handleDynamicMapUpdate(buffer);
                    break;
                default:
                    System.err.println("Unknown API Key: " + apiKey);
            }

        } catch (Exception e) {
            System.err.println("Failed to process request: " + e.getMessage());
            e.printStackTrace();
            buffer.clear();
        }
    }

    // ===================== API REQUEST HANDLERS ==========================

    private void handleProduceRequest(ByteBuffer buffer) throws IOException {
        DataDecoder.DecodedRecord record = DataDecoder.decode(buffer);
        System.out.println("--- INCOMING PRODUCE REQUEST ---");
        System.out.println("Topic: " + record.topic);
        topicManager.routeRecord(record.topic, record.key, record.payload);
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