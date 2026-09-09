package com.minikafka.client;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class ProducerRoutingTest {

    @Test
    @SuppressWarnings("unchecked")
    void testProducerFetchesMetadataAndRoutesCorrectly() throws Exception {
        ServerSocketChannel fakeController = ServerSocketChannel.open();
        fakeController.bind(new InetSocketAddress("localhost", 0));
        int controllerPort = fakeController.socket().getLocalPort();

        ServerSocketChannel fakeWorker = ServerSocketChannel.open();
        fakeWorker.bind(new InetSocketAddress("localhost", 0));
        int workerPort = fakeWorker.socket().getLocalPort();

        // 1. Thread-safe variables to capture what the Fake Worker receives
        AtomicReference<String> receivedRawData = new AtomicReference<>();
        CountDownLatch messageReceivedLatch = new CountDownLatch(1);

        Thread controllerThread = new Thread(() -> {
            try {
                SocketChannel client = fakeController.accept();
                ByteBuffer req = ByteBuffer.allocate(2);
                client.read(req); 
                
                ByteBuffer resp = ByteBuffer.allocate(12); 
                resp.putInt(1); 
                resp.putInt(0); 
                resp.putInt(workerPort); 
                resp.flip();
                client.write(resp);
            } catch (Exception e) {}
        });
        controllerThread.start();

        Thread workerThread = new Thread(() -> {
            try {
                SocketChannel client = fakeWorker.accept();
                ByteBuffer req = ByteBuffer.allocate(1024);
                int bytesRead = client.read(req); 
                
                if (bytesRead > 0) {
                    req.flip();
                    byte[] rawBytes = new byte[req.remaining()];
                    req.get(rawBytes);
                    // 2. Save the raw byte string to check its contents later
                    receivedRawData.set(new String(rawBytes));
                }
                
                ByteBuffer resp = ByteBuffer.allocate(1);
                resp.put((byte) 1);
                resp.flip();
                client.write(resp);
                
                // 3. Signal to the main thread that the message has arrived!
                messageReceivedLatch.countDown();
            } catch (Exception e) {}
        });
        workerThread.start();

        // ==========================================
        // ACT
        // ==========================================

        MiniKafkaProducer producer = new MiniKafkaProducer("localhost", controllerPort);

        Field mapField = MiniKafkaProducer.class.getDeclaredField("clusterMetadata");
        mapField.setAccessible(true);
        Map<Integer, Integer> metadata = (Map<Integer, Integer>) mapField.get(producer);
        
        assertEquals(1, metadata.size(), "Producer should have 1 partition in its map");
        assertEquals(workerPort, metadata.get(0), "Partition 0 should route directly to the fake worker port");

        assertDoesNotThrow(() -> {
            producer.send("test-topic", "key-for-p0", "hello routing".getBytes());
        }, "Producer should route successfully to the Worker without throwing exceptions");

        // ==========================================
        // ASSERT THE BROKER RECEIVED THE EXACT DATA
        // ==========================================
        
        // Wait up to 2 seconds for the network transfer to finish
        boolean messageArrived = messageReceivedLatch.await(2, TimeUnit.SECONDS);
        assertTrue(messageArrived, "Fake Worker did not receive the message in time!");

        String capturedData = receivedRawData.get();
        assertNotNull(capturedData, "Worker received empty data buffer");
        
        // The raw string will contain unprintable characters (for the integers/shorts), 
        // but the actual text components should be perfectly readable in the buffer.
        assertTrue(capturedData.contains("test-topic"), "Broker should have received the correct topic");
        assertTrue(capturedData.contains("key-for-p0"), "Broker should have received the correct routing key");
        assertTrue(capturedData.contains("hello routing"), "Broker should have received the correct payload");

        // Cleanup
        producer.close();
        fakeController.close();
        fakeWorker.close();
    }
}