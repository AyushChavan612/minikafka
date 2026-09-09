package com.minikafka.client;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerRoutingTest {

    @Test
    void testConsumerFetchesMetadataAndStreamsFromWorker() throws Exception {
        // 1. Setup Fake Controller & Worker on Random Free Ports
        ServerSocketChannel fakeController = ServerSocketChannel.open();
        fakeController.bind(new InetSocketAddress("localhost", 0));
        int controllerPort = fakeController.socket().getLocalPort();

        ServerSocketChannel fakeWorker = ServerSocketChannel.open();
        fakeWorker.bind(new InetSocketAddress("localhost", 0));
        int workerPort = fakeWorker.socket().getLocalPort();

        CountDownLatch testCompleteLatch = new CountDownLatch(1);

        // 2. Start Fake Controller (Handles Group Join, Metadata, and Offsets)
        Thread controllerThread = new Thread(() -> {
            try {
                SocketChannel client = fakeController.accept();
                
                // A. Handle JOIN_GROUP (Returns Partition 0)
                ByteBuffer joinReq = ByteBuffer.allocate(1024);
                client.read(joinReq);
                ByteBuffer joinResp = ByteBuffer.allocate(4);
                joinResp.putInt(0);
                joinResp.flip();
                client.write(joinResp);

                // B. Handle FETCH_METADATA (Points Partition 0 to Fake Worker)
                ByteBuffer metaReq = ByteBuffer.allocate(2);
                client.read(metaReq);
                ByteBuffer metaResp = ByteBuffer.allocate(12);
                metaResp.putInt(1); // 1 Partition
                metaResp.putInt(0); // Partition 0
                metaResp.putInt(workerPort); // Leader Port
                metaResp.flip();
                client.write(metaResp);

                // C. Handle FETCH_OFFSET (Returns offset 0)
                ByteBuffer offsetReq = ByteBuffer.allocate(1024);
                client.read(offsetReq);
                ByteBuffer offsetResp = ByteBuffer.allocate(8);
                offsetResp.putLong(0L);
                offsetResp.flip();
                client.write(offsetResp);
                
                // D. Handle COMMIT_OFFSET
                ByteBuffer commitReq = ByteBuffer.allocate(1024);
                client.read(commitReq);
                ByteBuffer commitResp = ByteBuffer.allocate(1);
                commitResp.put((byte) 1); // Success
                commitResp.flip();
                client.write(commitResp);

            } catch (Exception e) {}
        });
        controllerThread.start();

        // 3. Start Fake Worker (Handles actual data streaming)
        Thread workerThread = new Thread(() -> {
            try {
                SocketChannel client = fakeWorker.accept();
                
                // Handle FETCH
                ByteBuffer req = ByteBuffer.allocate(1024);
                client.read(req); 

                // Send back: Status(1) + Length + Payload
                byte[] payload = "distributed-hello".getBytes();
                ByteBuffer resp = ByteBuffer.allocate(1 + 4 + payload.length);
                resp.put((byte) 1);
                resp.putInt(payload.length);
                resp.put(payload);
                resp.flip();
                client.write(resp);
                
            } catch (Exception e) {}
        });
        workerThread.start();

        // ==========================================
        // ACT & ASSERT
        // ==========================================

        // 4. Initialize the Controller Client
        HighLevelNetworkClient adminClient = new HighLevelNetworkClient("localhost", controllerPort);
        
        // 5. Initialize the Consumer (Will automatically Join Group & Fetch Metadata)
        HighLevelConsumer consumer = new HighLevelConsumer(adminClient, "test-group", "test-topic");

        // 6. Poll for data! (Should hit the Fake Worker and return our payload)
        String payload = consumer.poll();
        
        assertNotNull(payload, "Consumer should have successfully fetched data");
        assertEquals("distributed-hello", payload, "Consumer did not receive the correct payload from the Worker");

        // Cleanup
        adminClient.close();
        fakeController.close();
        fakeWorker.close();
    }
}