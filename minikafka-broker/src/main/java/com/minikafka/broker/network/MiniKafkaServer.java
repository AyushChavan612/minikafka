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

  public MiniKafkaServer(int port) {
    this.port = port;
    this.topicManager = new TopicManager(3);
    this.groupCoordinator = new GroupCoordinator(); // Already initialized here!
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
      short apiKey = buffer.getShort();

      if (apiKey == RequestCodes.PRODUCE) {
        DataDecoder.DecodedRecord record = DataDecoder.decode(buffer);

        System.out.println("--- INCOMING PRODUCE REQUEST ---");
        System.out.println("Topic: " + record.topic);

        topicManager.routeRecord(record.topic, record.key, record.payload);

      } else if (apiKey == RequestCodes.FETCH) {
        System.out.println("--- INCOMING FETCH REQUEST ---");

        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        // Read the partition ID dynamically!
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();

        System.out
            .println("Requested Topic: " + topic + " | Partition: " + partitionId + " | Offset: " + offset);

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

      } else if (apiKey == RequestCodes.FETCH_OFFSET) {
        // 1. Read the request: GroupID, Topic, PartitionID
        int groupIdLen = buffer.getInt();
        byte[] groupIdBytes = new byte[groupIdLen];
        buffer.get(groupIdBytes);
        String groupId = new String(groupIdBytes);

        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();

        // 2. Look it up
        long currentOffset = groupCoordinator.fetchOffset(groupId, topic, partitionId);

        // 3. Send it back (8 bytes for a long)
        ByteBuffer responseBuffer = ByteBuffer.allocate(8);
        responseBuffer.putLong(currentOffset);
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
          clientChannel.write(responseBuffer);
        }

      } else if (apiKey == RequestCodes.COMMIT_OFFSET) {
        // 1. Read the request: GroupID, Topic, PartitionID, Offset
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

        // 2. Save it
        groupCoordinator.commitOffset(groupId, topic, partitionId, offset);

        // 3. Send a simple 1-byte success acknowledgment
        ByteBuffer responseBuffer = ByteBuffer.allocate(1);
        responseBuffer.put((byte) 1);
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
          clientChannel.write(responseBuffer);
        }

      } else if (apiKey == RequestCodes.JOIN_GROUP) {
        // 1. Read Topic, Group, and Consumer ID
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

        // 2. Register with Coordinator (Assuming 3 partitions for now)
        int assignedPartition = groupCoordinator.registerConsumer(groupId, topic, consumerId, 3);

        // 3. Reply with the assigned partition ID (4 bytes)
        ByteBuffer responseBuffer = ByteBuffer.allocate(4);
        responseBuffer.putInt(assignedPartition);
        responseBuffer.flip();
        while (responseBuffer.hasRemaining()) {
          clientChannel.write(responseBuffer);
        }
      } else {
        System.err.println("Unknown API Key: " + apiKey);
      }

    } catch (Exception e) {
      System.err.println("Failed to process request: " + e.getMessage());
      e.printStackTrace();
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
