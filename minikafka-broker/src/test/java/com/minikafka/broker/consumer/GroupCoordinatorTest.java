package com.minikafka.broker.consumer;

import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class GroupCoordinatorTest {

    @Test
    void testCommitAndFetchOffset() {
        assertDoesNotThrow(() -> {
            // 1. Connect to your running Broker
            SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("localhost", 9092));

            String groupId = "test-group";
            String topic = "system-events";
            int partitionId = 2;
            long offsetToCommit = 150L;

            byte[] groupBytes = groupId.getBytes();
            byte[] topicBytes = topic.getBytes();

            // ==========================================
            // TEST 1: COMMIT OFFSET (API Key 4)
            // ==========================================
            System.out.println("--- TESTING COMMIT OFFSET ---");
            ByteBuffer commitReq = ByteBuffer.allocate(22 + groupBytes.length + topicBytes.length);
            commitReq.putShort((short) 4); // COMMIT_OFFSET API KEY
            commitReq.putInt(groupBytes.length);
            commitReq.put(groupBytes);
            commitReq.putInt(topicBytes.length);
            commitReq.put(topicBytes);
            commitReq.putInt(partitionId);
            commitReq.putLong(offsetToCommit);
            
            commitReq.flip();
            while (commitReq.hasRemaining()) {
                socketChannel.write(commitReq);
            }

            // Read the 1-byte ACK
            ByteBuffer commitAck = ByteBuffer.allocate(1);
            socketChannel.read(commitAck);
            commitAck.flip();
            assertEquals(1, commitAck.get(), "Broker should return 1 (Success) for commit");
            System.out.println("Commit successful!");

            // ==========================================
            // TEST 2: FETCH OFFSET (API Key 3)
            // ==========================================
            System.out.println("--- TESTING FETCH OFFSET ---");
            ByteBuffer fetchReq = ByteBuffer.allocate(14 + groupBytes.length + topicBytes.length);
            fetchReq.putShort((short) 3); // FETCH_OFFSET API KEY
            fetchReq.putInt(groupBytes.length);
            fetchReq.put(groupBytes);
            fetchReq.putInt(topicBytes.length);
            fetchReq.put(topicBytes);
            fetchReq.putInt(partitionId);
            
            fetchReq.flip();
            while (fetchReq.hasRemaining()) {
                socketChannel.write(fetchReq);
            }

            // Read the 8-byte offset response
            ByteBuffer fetchResp = ByteBuffer.allocate(8);
            socketChannel.read(fetchResp);
            fetchResp.flip();
            
            long retrievedOffset = fetchResp.getLong();
            System.out.println("Retrieved Offset from Broker: " + retrievedOffset);
            
            assertEquals(offsetToCommit, retrievedOffset, "Broker should return the exact offset we just committed");

            socketChannel.close();
        });
    }
}